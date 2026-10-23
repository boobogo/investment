import json
import stat

import pytest

from pipeline import brain
from pipeline.config import ClaudeCfg

CFG = ClaudeCfg(retries=2, backoff_s=[30, 120])
SCHEMA = {"type": "object", "properties": {"cause": {"type": "string"}}}


class FakeBackend:
    """Stands in for Claude: returns the queued results in order and records each call."""

    name, model = "fake", "fake-model"

    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def complete(self, system, user, schema, web_search=False):
        self.calls.append({"system": system, "user": user, "web": web_search, "schema": schema})
        r = self.results.pop(0)
        return r(user) if callable(r) else r


def ok(data=None, **usage):
    return brain.LLMResult(ok=True, data=data if data is not None else {"cause": "x"}, usage=usage, duration_ms=1000)


FAIL = brain.LLMResult(ok=False, error="socket hang up", retryable=True)
LIMIT = brain.LLMResult(ok=False, error="You've hit your limit", usage_limited=True)


def call(backend, cfg=CFG):
    sleeps = []
    attempts = brain.complete_with_retry(backend, "sys", "user", SCHEMA, cfg, label="t", web_search=True, sleep=sleeps.append)
    return attempts, sleeps


def test_retry_with_backoff_then_success():
    attempts, sleeps = call(FakeBackend(FAIL, FAIL, ok()))
    assert [a.ok for a in attempts] == [False, False, True] and sleeps == [30, 120]


def test_retries_run_out():
    attempts, sleeps = call(FakeBackend(FAIL, FAIL, FAIL))
    assert len(attempts) == 3 and not attempts[-1].ok and sleeps == [30, 120]


def test_usage_limit_and_fatal_errors_are_not_retried():
    assert len(call(FakeBackend(LIMIT))[0]) == 1
    fatal = brain.LLMResult(ok=False, error="Invalid API key", retryable=False)
    assert len(call(FakeBackend(fatal))[0]) == 1


def test_web_search_is_passed_through():
    backend = FakeBackend(ok())
    call(backend)
    assert backend.calls[0]["web"] is True


# --- CLI backend ---


CLI_SUCCESS = {
    "type": "result", "subtype": "success", "is_error": False, "duration_ms": 1349, "num_turns": 9,
    "result": "{}", "structured_output": {"cause": "tariffs"}, "total_cost_usd": 0.0058,
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
    assert r.ok and r.data == {"cause": "tariffs"}
    assert r.model == "claude-sonnet-5-5"  # not the Haiku side call
    assert r.usage["cache_write_tokens"] == 1090 and r.usage["thinking_tokens"] == 7
    assert r.usage["side_models"] == {"claude-haiku-4-5-20251001": 912}
    assert r.usage["turns"] == 9


def test_cli_parse_falls_back_to_result_text(cli):
    out = {**CLI_SUCCESS, "structured_output": None, "result": json.dumps({"cause": "x"})}
    assert cli.parse(0, json.dumps(out), "", 10).data == {"cause": "x"}


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
    cmd = cli.command("SYS", SCHEMA, web_search=False)
    assert cmd[:2] == ["claude", "-p"]
    assert cmd[cmd.index("--tools") + 1] == "" and "WebSearch" not in cmd
    assert cmd[cmd.index("--system-prompt") + 1] == "SYS"
    assert {"--no-session-persistence", "--safe-mode", "--strict-mcp-config"} <= set(cmd)
    assert "--bare" not in cmd  # would ignore the Pro login
    assert cmd[cmd.index("--effort") + 1] == "high"
    web = cli.command("SYS", SCHEMA, web_search=True)
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
    r = backend.complete("SYS", "hello", SCHEMA)
    assert r.ok and r.data == {"key": None, "user": "hello"}


def test_cli_complete_missing_executable(tmp_path):
    backend = brain.ClaudeCLIBackend("m", None, 30, executable=str(tmp_path / "nope"))
    backend.workdir = tmp_path
    r = backend.complete("SYS", "x", SCHEMA)
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
