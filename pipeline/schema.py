"""The analyst's output contract: pydantic models plus the JSON schema sent to the model.

The JSON schema is kept deliberately simple (types, enums, required fields) so
both the CLI's --json-schema and the API's structured outputs accept it. Item
counts are stated in the descriptions and checked here instead.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

VERDICTS = ("bullish", "neutral", "bearish", "watch")
CONFIDENCE = ("low", "med", "high")


class Verdict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str
    verdict: Literal["bullish", "neutral", "bearish", "watch"]
    confidence: Literal["low", "med", "high"]
    # the prompt asks for exactly 3; a 2 or 4 is accepted rather than paying for a re-ask
    key_reasons: list[str] = Field(min_length=2, max_length=4)
    risks: list[str] = Field(min_length=1)
    what_would_change_my_mind: str = Field(min_length=1)
    data_gaps: list[str]


def _str_list(description: str) -> dict:
    return {"type": "array", "items": {"type": "string"}, "description": description}


VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string"},
        "verdict": {"type": "string", "enum": list(VERDICTS)},
        "confidence": {"type": "string", "enum": list(CONFIDENCE)},
        "key_reasons": _str_list("exactly 3 bullets, most important first"),
        "risks": _str_list("1-3 bullets"),
        "what_would_change_my_mind": {"type": "string"},
        "data_gaps": _str_list("empty if none"),
    },
    "required": [
        "ticker", "verdict", "confidence", "key_reasons", "risks",
        "what_would_change_my_mind", "data_gaps",
    ],
    "additionalProperties": False,
}  # fmt: skip

BATCH_SCHEMA = {
    "type": "object",
    "properties": {"verdicts": {"type": "array", "items": VERDICT_SCHEMA}},
    "required": ["verdicts"],
    "additionalProperties": False,
}


def parse_batch(data: object, expected: list[str]) -> tuple[dict[str, Verdict], list[str]]:
    """Validate a batch response entry by entry.

    Returns the valid verdicts for expected tickers, and a list of problems
    (missing or invalid tickers) that justify a re-ask.
    """
    problems: list[str] = []
    entries = data.get("verdicts") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return {}, ["response has no 'verdicts' list"]

    wanted = set(expected)
    good: dict[str, Verdict] = {}
    for entry in entries:
        ticker = entry.get("ticker") if isinstance(entry, dict) else None
        if ticker not in wanted:
            problems.append(f"unexpected ticker {ticker!r}")
            continue
        try:
            good[ticker] = Verdict.model_validate(entry)
        except ValidationError as e:
            errs = "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors())
            problems.append(f"{ticker}: {errs}")
    for t in expected:
        if t not in good and not any(p.startswith(f"{t}:") for p in problems):
            problems.append(f"{t}: missing from response")
    return good, problems
