"""The reasoning step: send packet batches to Claude and collect validated verdicts.

The model is reached through a small `LLMBackend` interface with two
implementations, selected by `llm.backend` in settings.yaml:
  - ClaudeCLIBackend: `claude -p` on the Pro subscription (default)
  - AnthropicAPIBackend: the Anthropic API (needs ANTHROPIC_API_KEY, `uv sync --extra api`)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from typing import Protocol

from pipeline import packets as packets_mod
from pipeline.config import ROOT, LLMCfg
from pipeline.schema import BATCH_SCHEMA, Verdict, parse_batch

log = logging.getLogger(__name__)

HORIZON_DAYS = 20  # trading days each verdict is about; evaluation scores against it

USAGE_LIMIT_RE = re.compile(r"usage limit|hit your (?:usage )?limit|limit reached|out of (?:extra )?usage", re.I)
RESET_RE = re.compile(r"resets?\s+(?:at\s+|in\s+)?([^\n.|]+)", re.I)


@dataclass
class LLMResult:
    ok: bool
    data: dict | None = None
    model: str | None = None
    usage: dict = field(default_factory=dict)  # input/output/cache/thinking tokens, cost_usd
    duration_ms: int = 0
    raw: str = ""
    error: str | None = None
    retryable: bool = False
    usage_limited: bool = False
    reset_hint: str | None = None


class LLMBackend(Protocol):
    name: str
    model: str

    def complete(self, system: str, user: str, schema: dict, web_search: bool = False) -> LLMResult: ...


def _limit_result(message: str, raw: str) -> LLMResult:
    m = RESET_RE.search(message)
    return LLMResult(
        ok=False, raw=raw, error=message.strip()[:500], usage_limited=True,
        reset_hint=m.group(1).strip() if m else None,
    )  # fmt: skip


class ClaudeCLIBackend:
    """`claude -p` with everything stripped out except the analyst prompt and the schema.

    --system-prompt replaces Claude Code's default prompt (~29k tokens -> ~1k),
    --tools "" removes all built-in tools, --strict-mcp-config with no config
    drops MCP servers and connectors, and --safe-mode ignores CLAUDE.md, hooks
    and plugins. (--bare would be leaner still, but it ignores the Pro login.)
    """

    name = "claude_cli"

    def __init__(self, model: str, effort: str | None, timeout_s: float, executable: str = "claude"):
        self.model, self.effort, self.timeout_s, self.executable = model, effort, timeout_s, executable
        self.workdir = ROOT / "data"  # neutral cwd: no project files for the CLI to pick up

    def command(self, system: str, schema: dict, web_search: bool) -> list[str]:
        cmd = [
            self.executable, "-p",
            "--model", self.model,
            "--output-format", "json",
            "--system-prompt", system,
            "--json-schema", json.dumps(schema),
            "--no-session-persistence",
            "--safe-mode",
            "--strict-mcp-config",
        ]  # fmt: skip
        if self.effort:
            cmd += ["--effort", self.effort]
        if web_search:
            cmd += ["--tools", "WebSearch", "--allowedTools", "WebSearch"]
        else:
            cmd += ["--tools", ""]
        return cmd

    def complete(self, system: str, user: str, schema: dict, web_search: bool = False) -> LLMResult:
        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)  # otherwise the CLI bills the API instead of the subscription
        self.workdir.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        try:
            proc = subprocess.run(
                self.command(system, schema, web_search),
                input=user, capture_output=True, text=True,
                timeout=self.timeout_s, env=env, cwd=self.workdir,
            )  # fmt: skip
        except subprocess.TimeoutExpired:
            return LLMResult(ok=False, error=f"timed out after {self.timeout_s:.0f}s", retryable=True)
        except FileNotFoundError:
            return LLMResult(ok=False, error=f"'{self.executable}' not found on PATH")
        elapsed = int((time.monotonic() - started) * 1000)
        return self.parse(proc.returncode, proc.stdout, proc.stderr, elapsed)

    def parse(self, returncode: int, stdout: str, stderr: str, elapsed_ms: int) -> LLMResult:
        try:
            out = json.loads(stdout)
        except json.JSONDecodeError:
            text = (stderr or stdout or f"exit code {returncode}").strip()
            if USAGE_LIMIT_RE.search(text):
                return _limit_result(text, stdout)
            auth = re.search(r"log ?in|not authenticated|invalid api key|oauth", text, re.I)
            return LLMResult(ok=False, raw=stdout, error=text[:500], retryable=not auth)

        result_text = out.get("result") or ""
        if out.get("is_error") or out.get("subtype") != "success":
            msg = str(result_text or out.get("subtype") or "unknown error")
            if USAGE_LIMIT_RE.search(msg):
                return _limit_result(msg, stdout)
            status = out.get("api_error_status")
            retryable = status is None or status == 429 or (isinstance(status, int) and status >= 500)
            return LLMResult(ok=False, raw=stdout, error=msg[:500], retryable=retryable)

        data = out.get("structured_output")
        if data is None:
            try:
                data = json.loads(result_text)
            except json.JSONDecodeError:
                data = None

        usage = out.get("usage") or {}
        model_usage = out.get("modelUsage") or {}
        # the CLI also makes a small side call to Haiku; report the main model
        main = next((m for m in model_usage if m.startswith(self.model)), None) or (
            max(model_usage, key=lambda m: model_usage[m].get("outputTokens", 0)) if model_usage else None
        )
        return LLMResult(
            ok=data is not None,
            data=data,
            model=main,
            usage={
                "input_tokens": usage.get("input_tokens", 0),
                "cache_write_tokens": usage.get("cache_creation_input_tokens", 0),
                "cache_read_tokens": usage.get("cache_read_input_tokens", 0),
                "output_tokens": usage.get("output_tokens", 0),
                "thinking_tokens": (usage.get("output_tokens_details") or {}).get("thinking_tokens", 0),
                "web_searches": (usage.get("server_tool_use") or {}).get("web_search_requests", 0),
                "cost_usd": out.get("total_cost_usd"),  # API-equivalent; not billed on Pro
                "side_models": {m: u.get("inputTokens", 0) + u.get("outputTokens", 0)
                                for m, u in model_usage.items() if m != main},
            },  # fmt: skip
            duration_ms=out.get("duration_ms") or elapsed_ms,
            raw=stdout,
            error=None if data is not None else "no JSON in response",
            retryable=data is None,
        )


class AnthropicAPIBackend:
    """Same contract over the Anthropic API, using structured outputs."""

    name = "api"

    def __init__(self, model: str, effort: str | None, timeout_s: float, web_search_max_uses: int):
        try:
            import anthropic
        except ImportError as e:
            raise SystemExit("llm.backend is 'api' but the SDK is missing: run `uv sync --extra api`") from e
        self.anthropic = anthropic
        self.client = anthropic.Anthropic(timeout=timeout_s, max_retries=0)  # retries happen in review()
        self.model, self.effort, self.web_search_max_uses = model, effort, web_search_max_uses

    def complete(self, system: str, user: str, schema: dict, web_search: bool = False) -> LLMResult:
        a = self.anthropic
        output_config: dict = {"format": {"type": "json_schema", "schema": schema}}
        if self.effort:
            output_config["effort"] = self.effort
        kwargs = dict(
            model=self.model,
            max_tokens=16000,
            system=system,
            messages=[{"role": "user", "content": user}],
            output_config=output_config,
            # if a safety classifier declines, re-run on Anthropic's recommended fallback model
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        )
        if web_search:
            kwargs["tools"] = [
                {"type": "web_search_20260209", "name": "web_search", "max_uses": self.web_search_max_uses}
            ]
        started = time.monotonic()
        try:
            resp = self.client.beta.messages.create(**kwargs)
        except a.RateLimitError as e:
            return LLMResult(ok=False, error=f"rate limited: {e.message}", retryable=True)
        except a.APIStatusError as e:
            return LLMResult(ok=False, error=f"API error {e.status_code}: {e.message}", retryable=e.status_code >= 500)
        except a.APIConnectionError as e:
            return LLMResult(ok=False, error=f"connection error: {e}", retryable=True)
        elapsed = int((time.monotonic() - started) * 1000)

        raw = resp.to_json()
        if resp.stop_reason == "refusal":
            return LLMResult(ok=False, raw=raw, error="model declined (refusal)", model=resp.model)
        texts = [b.text for b in resp.content if b.type == "text"]
        try:
            data = json.loads(texts[-1]) if texts else None
        except json.JSONDecodeError:
            data = None
        u = resp.usage
        return LLMResult(
            ok=data is not None,
            data=data,
            model=resp.model,
            usage={
                "input_tokens": u.input_tokens,
                "cache_write_tokens": u.cache_creation_input_tokens or 0,
                "cache_read_tokens": u.cache_read_input_tokens or 0,
                "output_tokens": u.output_tokens,
                "web_searches": getattr(u.server_tool_use, "web_search_requests", 0) if u.server_tool_use else 0,
                "cost_usd": None,
            },
            duration_ms=elapsed,
            raw=raw,
            error=None if data is not None else f"no JSON in response (stop_reason={resp.stop_reason})",
            retryable=data is None,
        )


def result_row(r: LLMResult, backend_name: str) -> dict:
    """The storage.save_call fields that come from one model result."""
    return {
        "reask": False, "web_search": False, "tickers": [], "backend": backend_name, "model": r.model,
        "ok": r.ok, "error": r.error, "usage_limited": r.usage_limited, "duration_ms": r.duration_ms,
        "usage": r.usage, "raw": r.raw,
    }  # fmt: skip


def make_backend(cfg: LLMCfg) -> LLMBackend:
    if cfg.backend == "api":
        return AnthropicAPIBackend(cfg.model, cfg.effort, cfg.timeout_s, cfg.web_search_max_uses)
    return ClaudeCLIBackend(cfg.model, cfg.effort, cfg.timeout_s)


# --- orchestration ---------------------------------------------------------------


@dataclass
class CallRecord:
    batch_no: int
    attempt: int
    tickers: list[str]
    reask: bool
    web_search: bool
    result: LLMResult


@dataclass
class ReviewOutcome:
    verdicts: dict[str, Verdict] = field(default_factory=dict)
    calls: list[CallRecord] = field(default_factory=list)
    source: dict[str, int] = field(default_factory=dict)  # ticker -> index in `calls` that produced its verdict
    pending: list[str] = field(default_factory=list)  # not reviewed; carry over to the next run
    stop_reason: str | None = None  # set when the run stopped calling early (usage limit, fatal error)

    def totals(self) -> dict:
        keys = ["input_tokens", "cache_write_tokens", "cache_read_tokens", "output_tokens", "thinking_tokens"]
        tot = {k: sum(c.result.usage.get(k, 0) or 0 for c in self.calls) for k in keys}
        tot["cost_usd"] = round(sum(c.result.usage.get("cost_usd") or 0 for c in self.calls), 6)
        tot["calls"] = len(self.calls)
        return tot


WEB_NOTE = (
    "For this batch you may use web search to check the headlines and look for material news "
    "after as_of. Cite what you find by source name in key_reasons. Searched facts may be used as "
    "evidence; the rule against inventing numbers still applies."
)


def build_user_message(batch: list[dict], *, as_of: str, benchmark: str, horizon_days: int = HORIZON_DAYS,
                       web_search: bool = False, problems: list[str] | None = None) -> str:  # fmt: skip
    tickers = [p["t"] for p in batch]
    lines = [
        f"As of: {as_of} (last US close). Benchmark: {benchmark}. Horizon: next {horizon_days} trading days.",
        f"Review these {len(tickers)} tickers, in this order: {', '.join(tickers)}",
    ]
    if web_search:
        lines.append(WEB_NOTE)
    if problems:
        lines.append(
            "Your previous answer for these tickers could not be used: " + "; ".join(problems)
            + ". Return a complete, valid entry for each ticker listed above."
        )  # fmt: skip
    lines += ["", "<packets>", *(packets_mod.dumps(p) for p in batch), "</packets>"]
    return "\n".join(lines)


def review(
    packets: list[dict],
    backend: LLMBackend,
    system: str,
    cfg: LLMCfg,
    *,
    benchmark: str,
    web_tickers: set[str] = frozenset(),
    sleep=time.sleep,
) -> ReviewOutcome:
    """Review packets in batches; never raises on model/CLI failures.

    Web-enabled tickers go into their own batches. A transient failure is
    retried with backoff; an invalid or incomplete answer is re-asked once for
    the affected tickers; a usage limit stops all further calls and leaves the
    remaining tickers pending for the next run.
    """
    outcome = ReviewOutcome()
    web = [p for p in packets if p["t"] in web_tickers]
    plain = [p for p in packets if p["t"] not in web_tickers]
    batches = [(b, True) for b in _chunks(web, cfg.batch_size)] + [(b, False) for b in _chunks(plain, cfg.batch_size)]

    for batch_no, (batch, use_web) in enumerate(batches, start=1):
        if outcome.stop_reason:
            outcome.pending += [p["t"] for p in batch]
            continue

        todo, problems = batch, None
        for reask in (False, True):
            if not todo:
                break
            res = _call_with_retry(backend, system, todo, cfg, batch_no, reask, use_web, benchmark, problems, outcome, sleep)
            if res.usage_limited:
                outcome.stop_reason = f"usage limit reached{f' (resets {res.reset_hint})' if res.reset_hint else ''}"
                log.error("batch %d: %s; stopping and carrying %d tickers over", batch_no, outcome.stop_reason, len(todo))
                break
            if not res.ok:
                log.error("batch %d: giving up after retries: %s", batch_no, res.error)
                if not res.retryable:
                    outcome.stop_reason = f"fatal LLM error: {res.error}"
                break
            good, problems = parse_batch(res.data, [p["t"] for p in todo])
            outcome.verdicts.update(good)
            outcome.source.update({t: len(outcome.calls) - 1 for t in good})
            if problems:
                log.warning("batch %d%s: %s", batch_no, " (re-ask)" if reask else "", "; ".join(problems))
            todo = [p for p in todo if p["t"] not in good]

        outcome.pending += [p["t"] for p in batch if p["t"] not in outcome.verdicts]
    return outcome


def complete_with_retry(
    backend: LLMBackend, system: str, user: str, schema: dict, cfg: LLMCfg, *,
    label: str, web_search: bool = False, sleep=time.sleep,
) -> list[LLMResult]:  # fmt: skip
    """One logical model call with retries for transient failures; returns every attempt (last one last)."""
    attempts = []
    for attempt in range(cfg.retries + 1):
        if attempt:
            wait = cfg.backoff_s[min(attempt - 1, len(cfg.backoff_s) - 1)] if cfg.backoff_s else 0
            log.warning("%s: retry %d/%d in %.0fs", label, attempt, cfg.retries, wait)
            sleep(wait)
        log.info("%s: calling %s (%s)%s", label, backend.name, backend.model, " with web search" if web_search else "")
        res = backend.complete(system, user, schema, web_search=web_search)
        attempts.append(res)
        if res.ok or res.usage_limited or not res.retryable:
            break
        log.warning("%s: attempt %d failed: %s", label, attempt + 1, res.error)
    return attempts


def _call_with_retry(backend, system, batch, cfg, batch_no, reask, use_web, benchmark, problems, outcome, sleep):
    user = build_user_message(
        batch, as_of=batch[0]["as_of"], benchmark=benchmark, web_search=use_web, problems=problems if reask else None
    )
    tickers = [p["t"] for p in batch]
    label = f"batch {batch_no} ({len(batch)} tickers{', re-ask' if reask else ''})"
    attempts = complete_with_retry(backend, system, user, BATCH_SCHEMA, cfg, label=label, web_search=use_web, sleep=sleep)
    for i, res in enumerate(attempts, start=1):
        outcome.calls.append(CallRecord(batch_no, i, tickers, reask, use_web, res))
    return attempts[-1]


def _chunks(items: list, n: int) -> list[list]:
    return [items[i : i + n] for i in range(0, len(items), n)]
