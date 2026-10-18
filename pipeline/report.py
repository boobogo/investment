"""Daily HTML report, built from the run log in SQLite.

Order: flagged names (non-neutral verdicts) first, then tickers at extreme
readings (whether or not they were sent), what changed since each ticker's
previous review, neutral verdicts, the full table and run stats.
Writes reports/<date>-<slot>.html and reports/latest.html.

Delivery: `report.delivery` in settings picks a `Deliverer`. Telegram sends a
short summary plus the HTML file; email is still a stub; "none" just writes files.
"""

from __future__ import annotations

import html
import json
import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Protocol

from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipeline import evaluate, screen, storage, telegram
from pipeline.config import Settings

log = logging.getLogger(__name__)

TEMPLATES = Path(__file__).parent / "templates"
CONF_RANK = {"high": 0, "med": 1, "low": 2}
VERDICT_RANK = {"watch": 0, "bullish": 1, "bearish": 2, "neutral": 3}
GLYPH = {"bullish": "▲", "bearish": "▼", "neutral": "●", "watch": "◆"}


# --- data ------------------------------------------------------------------------


def _verdict_view(row: sqlite3.Row, packet: dict | None, previous: sqlite3.Row | None) -> dict:
    packet = packet or {}
    v = {
        "ticker": row["ticker"],
        "name": packet.get("name", row["ticker"]),
        "verdict": row["verdict"],
        "confidence": row["confidence"],
        "key_reasons": json.loads(row["key_reasons_json"]),
        "risks": json.loads(row["risks_json"]),
        "what_would_change_my_mind": row["what_would_change_my_mind"],
        "data_gaps": json.loads(row["data_gaps_json"]),
        "price": row["price"],
        "as_of": row["as_of"],
        "ret_1d": packet.get("tech", {}).get("ret_1d"),
        "signals": packet.get("signals", []),
        "news": packet.get("news", [])[:3],
        "earnings_in_days": packet.get("earnings_in_days"),
        "held": packet.get("held", False),
        "previous": None,
    }
    if previous is not None:
        v["previous"] = {
            "verdict": previous["verdict"],
            "confidence": previous["confidence"],
            "as_of": previous["as_of"],
            "px_chg_pct": (row["price"] / previous["price"] - 1) * 100,
        }
    return v


def track_record(conn: sqlite3.Connection, settings: Settings) -> dict:
    """The main-horizon scorecard for the report (computed from the whole verdict history)."""
    cfg = settings.evaluate
    horizon = 20 if 20 in cfg.horizons else cfg.horizons[len(cfg.horizons) // 2]
    scored = evaluate.score_verdicts(conn, cfg, settings.data.benchmark)
    tables = evaluate.summarize(scored, horizon) if not scored.empty else {}
    rows = []
    if tables:
        for name, r in tables["by_verdict"].iterrows():
            rows.append({"label": name, "verdict": name, **r.to_dict()})
        for name, r in tables["compare"].iterrows():
            rows.append({"label": name, "verdict": None, "avg_excess": None, **r.to_dict()})
    return {
        "horizon": horizon,
        "rows": rows,
        "min_sample": cfg.min_sample,
        "first_maturity": None if tables else evaluate.first_maturity(conn, horizon),
        "benchmark": settings.data.benchmark,
    }


def load(conn: sqlite3.Connection, run_id: int, settings: Settings | None = None) -> dict:
    run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if run is None:
        raise ValueError(f"no run with id {run_id}")
    packet_rows = conn.execute("SELECT * FROM packets WHERE run_id = ? ORDER BY ticker", (run_id,)).fetchall()
    packets = {r["ticker"]: (json.loads(r["packet_json"]), bool(r["sent"])) for r in packet_rows}
    conditions = {r["ticker"]: json.loads(r["signals_json"] or "[]") for r in packet_rows}
    tickers = list(packets)
    verdict_rows = {r["ticker"]: r for r in conn.execute("SELECT * FROM verdicts WHERE run_id = ?", (run_id,))}
    previous = storage.last_verdicts(conn, tickers, before_run_id=run_id)
    latest = storage.last_verdicts(conn, tickers, before_run_id=run_id + 1)

    verdicts = [
        _verdict_view(row, packets.get(t, ({}, False))[0], previous.get(t)) for t, row in verdict_rows.items()
    ]
    verdicts.sort(key=lambda v: (CONF_RANK[v["confidence"]], VERDICT_RANK[v["verdict"]], v["ticker"]))
    flagged = [v for v in verdicts if v["verdict"] != "neutral"]
    neutral = [v for v in verdicts if v["verdict"] == "neutral"]
    changed = sorted(
        (v for v in verdicts
         if v["previous"] and (v["previous"]["verdict"], v["previous"]["confidence"]) != (v["verdict"], v["confidence"])),
        key=lambda v: v["ticker"],
    )  # fmt: skip
    first_reviews = sorted(v["ticker"] for v in verdicts if v["previous"] is None)

    table = []
    for t, (p, sent) in packets.items():
        tech = p.get("tech", {})
        last = latest.get(t)
        table.append({
            "ticker": t, "name": p.get("name", t), "px": p.get("px"), "sent": sent,
            "ret_1d": tech.get("ret_1d"), "ret_5d": tech.get("ret_5d"), "ret_20d": tech.get("ret_20d"),
            "ret_20d_vs_bench": tech.get("ret_20d_vs_bench"), "rsi14": tech.get("rsi14"), "z20": tech.get("z20"),
            "px_vs_sma200": tech.get("px_vs_sma200"), "atr_pct": tech.get("atr_pct"),
            "earnings_in_days": p.get("earnings_in_days"), "signals": p.get("signals", []),
            "held": p.get("held", False),
            "verdict": last["verdict"] if last else None, "confidence": last["confidence"] if last else None,
            "verdict_as_of": last["as_of"] if last else None,
            "verdict_is_new": bool(last) and last["run_id"] == run_id,
        })  # fmt: skip

    extreme_kinds = {*screen.EXTREMES, "ext_move"}
    extremes = []
    for t, conds in conditions.items():
        hits = [c for c in conds if isinstance(c, dict) and c.get("kind") in extreme_kinds]
        if hits:
            p, sent = packets[t]
            last = latest.get(t)
            extremes.append({
                "ticker": t, "name": p.get("name", t), "conditions": [c["label"] for c in hits], "sent": sent,
                "rsi14": p.get("tech", {}).get("rsi14"), "z20": p.get("tech", {}).get("z20"),
                "ext": p.get("ext"), "verdict": last["verdict"] if last else None,
                "confidence": last["confidence"] if last else None,
            })  # fmt: skip
    extremes.sort(key=lambda e: -max(abs((e["rsi14"] or 50) - 50) / 30, abs(e["z20"] or 0) / 2.5))

    calls = [
        {**dict(r), "n_tickers": len(json.loads(r["tickers_json"]))}
        for r in conn.execute("SELECT * FROM llm_calls WHERE run_id = ? ORDER BY id", (run_id,))
    ]
    run = dict(run)
    duration = None
    if run["finished_at"]:
        duration = (datetime.fromisoformat(run["finished_at"]) - datetime.fromisoformat(run["started_at"])).seconds
    extras = json.loads(run.get("extras_json") or "{}")
    return {
        "run": run,
        "highlights": storage.load_highlights(conn, run_id),
        "earnings": extras.get("earnings", []),
        "movers": extras.get("movers", []),
        "held": extras.get("held", []),
        "news_window": extras.get("window"),
        "duration_s": duration,
        "flagged": flagged,
        "neutral": neutral,
        "extremes": extremes,
        "changed": changed,
        "first_reviews": first_reviews,
        "table": table,
        "calls": calls,
        "pending": storage.get_pending(conn),
        "prompt_tokens": sum(run.get(k) or 0 for k in ("input_tokens", "cache_write_tokens", "cache_read_tokens")),
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        "track": track_record(conn, settings) if settings else None,
    }


# --- rendering -------------------------------------------------------------------


def fmt_pct(x, decimals: int = 1, signed: bool = True) -> str:
    if x is None:
        return "–"
    sign = ("+" if x > 0 else "−" if x < 0 else "") if signed else ("−" if x < 0 else "")
    return f"{sign}{abs(x):.{decimals}f}%"


def fmt_num(x, decimals: int = 2) -> str:
    return "–" if x is None else f"{x:,.{decimals}f}"


def _env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES),
        autoescape=select_autoescape(["html", "j2"]),  # headlines are untrusted text
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters.update(pct=fmt_pct, num=fmt_num)
    env.globals.update(glyph=GLYPH)
    return env


def render(data: dict) -> str:
    return _env().get_template("daily.html.j2").render(**data)


def summary_text(data: dict) -> str:
    """Short plain-text digest (console now; Telegram/email body later)."""
    run = data["run"]
    lines = [
        f"Market research {run['run_date']} {run['slot']} (as of {run['as_of']}): "
        f"{run['n_verdicts'] or 0} reviewed, {len(data['flagged'])} flagged, {len(data['extremes'])} at extremes"
    ]
    for v in data["flagged"]:
        lines.append(f"  {v['verdict'].upper()} ({v['confidence']}) {v['ticker']}: {v['key_reasons'][0]}")
    for e in data["extremes"]:
        lines.append(f"  EXTREME {e['ticker']}: {'; '.join(e['conditions'])}")
    for v in data["changed"]:
        lines.append(f"  changed {v['ticker']}: {v['previous']['verdict']} -> {v['verdict']}")
    if data["pending"]:
        lines.append(f"  pending: {', '.join(sorted(data['pending']))}")
    return "\n".join(lines)


DIGEST_TITLES = {"morning": "Morning digest", "preopen": "Pre-open digest", "weekly": "Week ahead"}


def telegram_summary(data: dict) -> str:
    """The digest message for Telegram (HTML; every piece of outside text is escaped)."""
    esc = telegram.esc
    run = data["run"]
    lines = [
        f"<b>{DIGEST_TITLES.get(run['slot'], 'Market research')}</b> · {run['run_date']}",
        f"<i>US close {run['as_of']} · {run['n_verdicts'] or 0} reviewed · {len(data['flagged'])} flagged · "
        f"{len(data['extremes'])} at extremes</i>",
    ]
    if data.get("highlights"):
        lines += ["", "<b>News highlights</b>"]
        for i, h in enumerate(data["highlights"], start=1):
            title = esc(h["title"])
            if h.get("url"):
                title = f'<a href="{html.escape(h["url"], quote=True)}">{title}</a>'
            tickers = f" ({esc(', '.join(h['tickers']))})" if h["tickers"] else ""
            lines.append(f"{i}. {title}{tickers}: {esc(h['why'])}")
    if data.get("earnings"):
        lines += ["", "<b>Earnings</b>"]
        for e in data["earnings"]:
            parts = [f"• {e['date']} <b>{esc(e['symbol'])}</b>", esc(e["when"]),
                     f"(Yahoo says {e['yahoo_date']})" if e.get("yahoo_date") else ""]  # fmt: skip
            lines.append(" ".join(p for p in parts if p))
    if data.get("movers"):
        lines += ["", "<b>Last week vs the benchmark</b>"]
        lines.append(" · ".join(f"{esc(m['symbol'])} {fmt_pct(m['vs_bench_pct'])}" for m in data["movers"][:6]))
    if data.get("held"):
        lines += ["", f"<b>Held back overnight ({len(data['held'])})</b>"]
        lines += [f"• {esc(a['text'][:200])}" for a in data["held"][:5]]
    if data["flagged"]:
        lines += ["", "<b>Flagged</b>"]
        lines += [
            f"{GLYPH[v['verdict']]} <b>{esc(v['ticker'])}</b> {v['verdict']} ({v['confidence']}): {esc(v['key_reasons'][0])}"
            for v in data["flagged"]
        ]
    if data["extremes"]:
        lines += ["", "<b>At extremes</b>"]
        lines += [f"• <b>{esc(e['ticker'])}</b>: {esc('; '.join(e['conditions']))}" for e in data["extremes"]]
    if data["changed"]:
        lines += ["", "<b>Changed since last review</b>"]
        lines += [f"• {esc(v['ticker'])}: {v['previous']['verdict']} → {v['verdict']}" for v in data["changed"]]
    if not (data["flagged"] or data["extremes"] or data["changed"] or data.get("highlights")):
        lines += ["", "Nothing unusual in this run."]
    if data["pending"]:
        lines += ["", f"<i>Carried over to the next run: {esc(', '.join(sorted(data['pending'])))}</i>"]
    return "\n".join(lines)


def output_path(reports_dir: Path, run: dict) -> Path:
    suffix = run["slot"] if run["slot"] != "manual" else f"manual-{run['id']}"
    return reports_dir / f"{run['run_date']}-{suffix}.html"


def build(conn: sqlite3.Connection, run_id: int, reports_dir: Path, settings: Settings | None = None) -> tuple[Path, dict]:
    data = load(conn, run_id, settings)
    out = output_path(reports_dir, data["run"])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(data), encoding="utf-8")
    shutil.copyfile(out, reports_dir / "latest.html")
    return out, data


# --- delivery ----------------------------------------------------------------------


class Deliverer(Protocol):
    def send(self, data: dict, html_path: Path) -> tuple[str, int | None]:
        """Deliver a report; returns (the message text, the delivered message id if any)."""
        ...


class TelegramDeliverer:
    """Summary message, then the HTML report as a file (needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)."""

    def __init__(self, settings: Settings, client: telegram.Telegram | None = None):
        self.settings, self.client = settings, client

    def send(self, data: dict, html_path: Path) -> tuple[str, int | None]:
        client = self.client or telegram.Telegram.from_env(self.settings.telegram)
        text = telegram_summary(data)
        ids = client.send(text)
        run = data["run"]
        client.send_document(html_path, caption=f"Full report · {run['run_date']} {run['slot']}")
        return text, ids[0]


class EmailDeliverer:
    """Planned: send the summary with the HTML report attached over SMTP (needs host and credentials)."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def send(self, data: dict, html_path: Path) -> tuple[str, int | None]:
        raise NotImplementedError("email delivery is not implemented yet")


def make_deliverer(name: str, settings: Settings) -> Deliverer | None:
    if name == "none":
        return None
    return {"telegram": TelegramDeliverer, "email": EmailDeliverer}[name](settings)
