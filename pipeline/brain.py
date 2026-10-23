"""Claude calls through the `claude` CLI on the Pro subscription.

A small `LLMBackend` interface keeps the rest of the code testable: tests pass a
fake backend, the watcher uses ClaudeCLIBackend. Every call returns structured
JSON (validated by the CLI against a schema) and may use web search.
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

from pipeline.config import ROOT, ClaudeCfg

log = logging.getLogger(__name__)

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
    """`claude -p` with everything stripped out except our prompt, the schema and (optionally) web search.

    --system-prompt replaces Claude Code's default prompt (~29k tokens -> ~1k),
    --tools limits the built-in tools to WebSearch (or none), --strict-mcp-config
    with no config drops MCP servers and connectors, and --safe-mode ignores
    CLAUDE.md, hooks and plugins. (--bare would be leaner still, but it ignores
    the Pro login.)
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
                # the CLI's WebSearch tool doesn't show up in server_tool_use; turns = tool rounds + 1
                "turns": out.get("num_turns"),
                "cost_usd": out.get("total_cost_usd"),  # API-equivalent; not billed on Pro
                "side_models": {m: u.get("inputTokens", 0) + u.get("outputTokens", 0)
                                for m, u in model_usage.items() if m != main},
            },  # fmt: skip
            duration_ms=out.get("duration_ms") or elapsed_ms,
            raw=stdout,
            error=None if data is not None else "no JSON in response",
            retryable=data is None,
        )


def complete_with_retry(
    backend: LLMBackend, system: str, user: str, schema: dict, cfg: ClaudeCfg, *,
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


def make_backend(cfg: ClaudeCfg) -> LLMBackend:
    return ClaudeCLIBackend(cfg.model, cfg.effort, cfg.timeout_s)
