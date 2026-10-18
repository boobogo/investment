"""News feeds beyond the per-ticker RSS, and the free pre-filter (no model involved).

Digests use it to pick highlight candidates; the live watcher will use the same
score to decide which fresh headlines deserve the model's judgement.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from pipeline import news
from pipeline.config import NewsCfg, NewsWatchCfg, Ticker
from pipeline.finnhub import Finnhub, FinnhubError

log = logging.getLogger(__name__)


def from_finnhub(d: dict, category: str, watchlist: list[Ticker]) -> news.NewsItem:
    source = d.get("source") or "Finnhub"
    title = re.sub(rf"\s+-\s+{re.escape(source)}$", "", news.clean_text(d.get("headline")))
    summary = news.clean_text(d.get("summary")) or None
    related = {s.strip().upper() for s in (d.get("related") or "").split(",") if s.strip()}
    tickers = [
        t.symbol for t in watchlist
        if (category == "general" and t.symbol in related)
        or news.mention_matcher(t)(title) or news.mention_matcher(t)(summary)
    ]  # fmt: skip
    return news.NewsItem(
        title=title,
        source=source,
        published=datetime.fromtimestamp(d["datetime"], timezone.utc),
        url=d.get("url", ""),
        snippet=summary,
        provider="finnhub",
        provider_id=str(d.get("id") or d.get("url")),
        category=category,
        tickers=tickers,
    )


def fetch_finnhub(
    fh: Finnhub, watchlist: list[Ticker], cfg: NewsWatchCfg, since: datetime, now: datetime
) -> tuple[list[news.NewsItem], list[str]]:
    """Market news plus company news for each watchlist ticker, published since `since`.

    `from_finnhub` tags tickers only when the headline or summary names the
    company (or Finnhub lists it as related for general news).
    """
    items, errors = [], []
    if cfg.finnhub_general:
        try:
            items += [from_finnhub(d, "general", watchlist) for d in fh.market_news("general")]
        except FinnhubError as e:
            errors.append(f"market news: {e}")
    if cfg.finnhub_company:
        for t in watchlist:
            try:
                for d in fh.company_news(t.symbol, since.date(), now.date()):
                    it = from_finnhub(d, "company", watchlist)
                    # Finnhub's company feed also returns general market pieces; keep only ones naming the company
                    if t.symbol in it.tickers:
                        items.append(it)
            except FinnhubError as e:
                errors.append(f"{t.symbol} company news: {e}")
    for e in errors:
        log.warning("finnhub: %s", e)
    return [i for i in items if i.published >= since and i.title], errors


class Prefilter:
    """Scores how likely a headline is to matter (None = drop it), and decides which fresh
    headlines are strong enough to go to the model for an instant alert."""

    def __init__(self, news_cfg: NewsCfg, cfg: NewsWatchCfg, watchlist: list[Ticker] = ()):
        self.matchers = {t.symbol: news.mention_matcher(t) for t in watchlist}
        self.skip = [re.compile(p) for p in news_cfg.skip_title_patterns]
        self.blocked = [s.lower() for s in news_cfg.blocked_sources + cfg.skip_sources]
        self.preferred = [s.lower() for s in news_cfg.preferred_sources] + ["reuters", "bloomberg", "cnbc"]
        self.impact = [re.compile(p) for p in cfg.impact_patterns]
        self.macro = [re.compile(p) for p in cfg.macro_patterns]
        self.low_quality = [s.lower() for s in cfg.low_quality_sources]
        self.clickbait = [re.compile(p) for p in cfg.clickbait_patterns]

    def is_breaking_candidate(self, it: news.NewsItem, now: datetime, max_age_minutes: float) -> bool:
        """Strict rule for the live path (digests only rank lower; this excludes):
        fresh, not clickbait or a low-quality outlet, and either
          - a watchlist company named in the headline itself, with a high-impact word, or
          - market news from a top outlet with a market-moving topic in the headline."""
        source = it.source.lower()
        if not 0 <= (now - it.published).total_seconds() <= max_age_minutes * 60:
            return False
        if self.score(it, now) is None or any(q in source for q in self.low_quality):
            return False
        if any(p.search(it.title) for p in self.clickbait):
            return False
        company = bool(it.tickers) and it.category == "company" and self.named_in_title(it)
        if company and any(p.search(it.title) for p in self.impact):
            return True
        top_outlet = any(p in source for p in self.preferred)
        return it.category == "general" and top_outlet and any(p.search(it.title) for p in self.macro + self.impact)

    def named_in_title(self, it: news.NewsItem) -> bool:
        return any(m(it.title) for m in self.matchers_for(it.tickers))

    def matchers_for(self, tickers: list[str]):
        return [self.matchers[t] for t in tickers if t in self.matchers]

    def score(self, it: news.NewsItem, now: datetime) -> float | None:
        source = it.source.lower()
        if any(p.search(it.title) for p in self.skip) or any(b in source for b in self.blocked):
            return None
        s = 0.0
        if any(p in source for p in self.preferred):
            s += 2
        if any(p.search(it.title) for p in self.impact):
            s += 3
        elif it.snippet and any(p.search(it.snippet) for p in self.impact):
            s += 1
        if any(p.search(it.title) for p in self.macro):
            s += 2
        if it.tickers:
            s += 2
        if it.snippet:
            s += 0.5
        if any(q in source for q in self.low_quality):
            s -= 2
        if any(p.search(it.title) for p in self.clickbait):
            s -= 2
        age_h = max(0.0, (now - it.published).total_seconds() / 3600)
        return s + max(0.0, 1 - age_h / 24)  # fresher first among equals


def rank_stories(
    items: list[news.NewsItem], prefilter: Prefilter, news_cfg: NewsCfg, now: datetime, limit: int
) -> list[tuple[float, list[news.NewsItem]]]:
    """Score, group into stories (best item leads), and return the top stories with their score.

    A story covered by several outlets gets a bonus: breadth of coverage is a cheap
    signal of importance.
    """
    scored = [(s, it) for it in items if (s := prefilter.score(it, now)) is not None]
    scored.sort(key=lambda x: (x[0], x[1].published), reverse=True)
    score_of = {id(it): s for s, it in scored}
    stories = news.cluster_stories([it for _, it in scored], set(), news_cfg, lead_only=True)
    ranked = [(score_of[id(st[0])] + 0.5 * min(len(st) - 1, 3), st) for st in stories]
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[:limit]
