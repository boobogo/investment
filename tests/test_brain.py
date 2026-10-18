import json
import stat

import pytest

from pipeline import brain
from pipeline.config import LLMCfg
from pipeline.schema import BATCH_SCHEMA, VERDICT_SCHEMA, Verdict, parse_batch

CFG = LLMCfg(batch_size=2, retries=2, backoff_s=[30, 120])


def verdict(t, v="neutral", conf="low", reasons=3):
    return {
        "ticker": t, "verdict": v, "confidence": conf,
        "key_reasons": [f"reason {i}" for i in range(reasons)],
        "risks": ["risk"], "what_would_change_my_mind": "a close above SMA50", "data_gaps": [],
    }  # fmt: skip


def packet(t):
    return {"t": t, "as_of": "2026-10-15", "px": 100.0}


class FakeBackend:
    name, model = "fake", "fake-model"

    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def complete(self, system, user, schema, web_search=False):
        self.calls.append({"user": user, "web": web_search})
        r = self.results.pop(0)
        return r(user) if callable(r) else r


def ok(*tickers, **kw):
    return brain.LLMResult(ok=True, data={"verdicts": [verdict(t, **kw) for t in tickers]}, usage={"output_tokens": 10})


def run(backend, tickers, **kw):
    sleeps = []
    out = brain.review([packet(t) for t in tickers], backend, "sys", CFG, benchmark="SPY", sleep=sleeps.append, **kw)
    return out, sleeps


# --- schema ---


def test_schema_matches_model_fields():
    assert set(VERDICT_SCHEMA["properties"]) == set(Verdict.model_fields)
    assert set(VERDICT_SCHEMA["required"]) == set(Verdict.model_fields)
    assert BATCH_SCHEMA["properties"]["verdicts"]["items"] is VERDICT_SCHEMA


def test_parse_batch():
    data = {"verdicts": [
        verdict("AAPL"),
        verdict("MSFT", v="strong buy"),  # invalid enum
        verdict("NVDA", reasons=2),  # tolerated
        verdict("TSLA"),  # not asked for
        verdict("AMZN", reasons=6),  # too many
    ]}  # fmt: skip
    good, problems = parse_batch(data, ["AAPL", "MSFT", "NVDA", "AMZN", "KO"])
    assert set(good) == {"AAPL", "NVDA"}
    text = " | ".join(problems)
    assert "MSFT: verdict" in text and "AMZN: key_reasons" in text
    assert "unexpected ticker 'TSLA'" in text and "KO: missing from response" in text


def test_parse_batch_garbage():
    assert parse_batch("nope", ["AAPL"]) == ({}, ["response has no 'verdicts' list"])
    assert parse_batch({"verdicts": "x"}, ["AAPL"])[1] == ["response has no 'verdicts' list"]


# --- user message ---


def test_user_message():
    msg = brain.build_user_message([packet("AAPL"), packet("MSFT")], as_of="2026-10-15", benchmark="SPY")
    assert "in this order: AAPL, MSFT" in msg and "Benchmark: SPY" in msg and "web search" not in msg
    lines = msg.split("<packets>\n")[1].split("\n</packets>")[0].splitlines()
    assert [json.loads(line)["t"] for line in lines] == ["AAPL", "MSFT"]
    assert "web search" in brain.build_user_message([packet("A")], as_of="x", benchmark="SPY", web_search=True)


# --- review orchestration ---


def test_review_happy_path_batches():
    backend = FakeBackend(ok("A", "B"), ok("C"))
    out, sleeps = run(backend, ["A", "B", "C"])
    assert set(out.verdicts) == {"A", "B", "C"} and not out.pending and not sleeps
    assert len(backend.calls) == 2 and out.totals()["output_tokens"] == 20


def test_review_reasks_once_for_missing_tickers():
    backend = FakeBackend(ok("A"), ok("B"))
    out, _ = run(backend, ["A", "B"])
    assert set(out.verdicts) == {"A", "B"} and out.source == {"A": 0, "B": 1}
    reask_msg = backend.calls[1]["user"]
    assert "in this order: B" in reask_msg and "B: missing from response" in reask_msg
    assert [c.reask for c in out.calls] == [False, True]


def test_review_gives_up_after_one_reask():
    backend = FakeBackend(ok("A"), ok("A"))  # re-ask still leaves B out
    out, _ = run(backend, ["A", "B"])
    assert set(out.verdicts) == {"A"} and out.pending == ["B"] and out.stop_reason is None


def test_review_retries_transient_failures_with_backoff():
    fail = brain.LLMResult(ok=False, error="timed out", retryable=True)
    backend = FakeBackend(fail, fail, ok("A", "B"))
    out, sleeps = run(backend, ["A", "B"])
    assert set(out.verdicts) == {"A", "B"} and sleeps == [30, 120]
    assert [c.attempt for c in out.calls] == [1, 2, 3]


def test_review_exhausted_retries_leave_batch_pending_but_continue():
    fail = brain.LLMResult(ok=False, error="timed out", retryable=True)
    backend = FakeBackend(fail, fail, fail, ok("C"))
    out, _ = run(backend, ["A", "B", "C"])
    assert out.pending == ["A", "B"] and set(out.verdicts) == {"C"} and out.stop_reason is None


def test_review_stops_on_usage_limit():
    limit = brain.LLMResult(ok=False, error="usage limit", usage_limited=True, reset_hint="3pm")
    backend = FakeBackend(ok("A", "B"), limit)
    out, sleeps = run(backend, ["A", "B", "C", "D", "E"])
    assert set(out.verdicts) == {"A", "B"} and out.pending == ["C", "D", "E"]
    assert out.stop_reason == "usage limit reached (resets 3pm)"
    assert len(backend.calls) == 2 and not sleeps  # no retries, no further batches


def test_review_stops_on_fatal_error():
    fatal = brain.LLMResult(ok=False, error="please log in", retryable=False)
    out, _ = run(FakeBackend(fatal), ["A", "B", "C"])
    assert out.pending == ["A", "B", "C"] and out.stop_reason.startswith("fatal")


def test_review_web_tickers_get_their_own_batch():
    backend = FakeBackend(ok("C"), ok("A", "B"))
    out, _ = run(backend, ["A", "B", "C"], web_tickers={"C"})
    assert [c["web"] for c in backend.calls] == [True, False]
    assert set(out.verdicts) == {"A", "B", "C"}


# --- CLI backend ---


CLI_SUCCESS = {
    "type": "result", "subtype": "success", "is_error": False, "duration_ms": 1349,
    "result": "{}", "structured_output": {"verdicts": [verdict("AAPL")]}, "total_cost_usd": 0.0058,
    "usage": {"input_tokens": 2, "cache_creation_input_tokens": 1090, "cache_read_input_tokens": 0,
              "output_tokens": 53, "output_tokens_details": {"thinking_tokens": 7},
              "server_tool_use": {"web_search_requests": 0}},
    "modelUsage": {"claude-haiku-4-5-20251001": {"inputTokens": 899, "outputTokens": 13},
                   "claude-sonnet-5-5": {"inputTokens": 2, "outputTokens": 53}},
}  # fmt: skip


@pytest.fixture
def cli():
    return brain.ClaudeCLIBackend("claude-sonnet-5-5", "high", 60)


def test_cli_parse_success(cli):
    r = cli.parse(0, json.dumps(CLI_SUCCESS), "", 10)
    assert r.ok and r.data["verdicts"][0]["ticker"] == "AAPL"
    assert r.model == "claude-sonnet-5-5"  # not the Haiku side call
    assert r.usage["cache_write_tokens"] == 1090 and r.usage["thinking_tokens"] == 7
    assert r.usage["side_models"] == {"claude-haiku-4-5-20251001": 912}


def test_cli_parse_falls_back_to_result_text(cli):
    out = {**CLI_SUCCESS, "structured_output": None, "result": json.dumps({"verdicts": []})}
    assert cli.parse(0, json.dumps(out), "", 10).data == {"verdicts": []}


@pytest.mark.parametrize("message,hint", [
    ("Claude AI usage limit reached|1760650000", None),
    ("You've hit your limit · resets 3pm (Europe/Prague)", "3pm (Europe/Prague)"),
])  # fmt: skip
def test_cli_parse_usage_limit(cli, message, hint):
    out = {"type": "result", "subtype": "success", "is_error": True, "result": message}
    r = cli.parse(1, json.dumps(out), "", 10)
    assert r.usage_limited and not r.ok and r.reset_hint == hint
    # same message on stderr without JSON
    assert cli.parse(1, "", message, 10).usage_limited


def test_cli_parse_errors(cli):
    assert not cli.parse(1, "", "Invalid API key · Please run /login", 10).retryable
    assert cli.parse(1, "", "socket hang up", 10).retryable
    overloaded = {"subtype": "success", "is_error": True, "result": "Overloaded", "api_error_status": 529}
    assert cli.parse(1, json.dumps(overloaded), "", 10).retryable
    bad_request = {"subtype": "success", "is_error": True, "result": "bad", "api_error_status": 400}
    assert not cli.parse(1, json.dumps(bad_request), "", 10).retryable


def test_cli_command_flags(cli):
    cmd = cli.command("SYS", BATCH_SCHEMA, web_search=False)
    assert cmd[:2] == ["claude", "-p"]
    assert cmd[cmd.index("--tools") + 1] == "" and "WebSearch" not in cmd
    assert cmd[cmd.index("--system-prompt") + 1] == "SYS"
    assert {"--no-session-persistence", "--safe-mode", "--strict-mcp-config"} <= set(cmd)
    assert "--bare" not in cmd  # would ignore the Pro login
    assert cmd[cmd.index("--effort") + 1] == "high"
    web = cli.command("SYS", BATCH_SCHEMA, web_search=True)
    assert web[web.index("--tools") + 1] == "WebSearch" and "--allowedTools" in web


def test_cli_complete_strips_api_key_and_uses_stdin(tmp_path, monkeypatch):
    fake = tmp_path / "fake-claude"
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "user = sys.stdin.read()\n"
        "print(json.dumps({'subtype': 'success', 'is_error': False, 'result': '',\n"
        "  'structured_output': {'key': os.environ.get('ANTHROPIC_API_KEY'), 'user': user}}))\n"
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-should-not-leak")
    backend = brain.ClaudeCLIBackend("m", None, 30, executable=str(fake))
    backend.workdir = tmp_path
    r = backend.complete("SYS", "hello packets", BATCH_SCHEMA)
    assert r.ok and r.data == {"key": None, "user": "hello packets"}


def test_cli_complete_missing_executable(tmp_path):
    backend = brain.ClaudeCLIBackend("m", None, 30, executable=str(tmp_path / "nope"))
    backend.workdir = tmp_path
    r = backend.complete("SYS", "x", BATCH_SCHEMA)
    assert not r.ok and not r.retryable and "not found" in r.error


def test_load_env(tmp_path, monkeypatch):
    from pipeline import config

    env = tmp_path / ".env"
    env.write_text('# comment\nA_KEY=abc\nexport B_KEY="quoted value"\nC_KEY=keep-mine\n\nnot a line\n')
    monkeypatch.delenv("A_KEY", raising=False)
    monkeypatch.delenv("B_KEY", raising=False)
    monkeypatch.setenv("C_KEY", "already-set")
    config.load_env(env)
    import os

    assert (os.environ["A_KEY"], os.environ["B_KEY"], os.environ["C_KEY"]) == ("abc", "quoted value", "already-set")
    with pytest.raises(config.MissingSecret, match="NOPE_KEY"):
        config.secret("NOPE_KEY")
