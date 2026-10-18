"""Recent headlines per ticker from free RSS feeds: Yahoo Finance and Google News.

(yfinance's own news endpoint currently returns 404, so the RSS feeds are used directly.)

Fetching is kept separate from `select()`, which applies the relevance filter,
listicle filter, ranking and dedup, so the rules can be tested on fixtures.
"""

from __future__ import annotations

import html
import logging
import math
import re
import sqlite3
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import feedparser
import requests

from pipeline import storage
from pipeline.config import NewsCfg, Ticker

log = logging.getLogger(__name__)

YAHOO_RSS = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
GOOGLE_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
USER_AGENT = "Mozilla/5.0 (compatible; market-research-pipeline/0.1)"


@dataclass
class NewsItem:
    title: str
    source: str
    published: datetime  # UTC
    url: str = ""
    snippet: str | None = None
    key: str = ""  # normalised title, used for dedup and the seen-state
    fresh: bool = False  # published within cfg.fresh_hours
    new: bool = False  # not shown in an earlier run
    provider: str = ""  # finnhub | yahoo | google
    provider_id: str = ""  # the provider's id (or url) for storage dedup
    category: str = ""  # general | company
    tickers: list[str] = field(default_factory=list)  # watchlist tickers it concerns

    def __post_init__(self):
        self.key = self.key or normalize_title(self.title)


@dataclass
class NewsResult:
    items: list[NewsItem]
    errors: list[str] = field(default_factory=list)
    n_raw: int = 0

    @property
    def has_fresh_new(self) -> bool:
        return any(i.fresh and i.new for i in self.items)


# --- parsing -------------------------------------------------------------------


def clean_text(text: str | None) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def normalize_title(title: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", title.lower().replace("&", " and ")).split())


def _published(entry) -> datetime | None:
    t = entry.get("published_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None


def parse_yahoo(entries) -> list[NewsItem]:
    """Yahoo RSS has real summaries but no publisher name, so use the link's domain."""
    items = []
    for e in entries:
        pub, title = _published(e), clean_text(e.get("title"))
        if not (pub and title):
            continue
        url = e.get("link", "")
        domain = urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
        snippet = clean_text(e.get("summary"))
        items.append(
            NewsItem(
                title=title,
                source="Yahoo Finance" if domain.endswith("yahoo.com") else domain,
                published=pub,
                url=url,
                snippet=snippet if normalize_title(snippet) != normalize_title(title) else None,
                provider="yahoo",
                provider_id=url,
            )
        )
    return items


def parse_google(entries) -> list[NewsItem]:
    """Google News titles end in " - <Source>"; its summary only repeats the title, so no snippet."""
    items = []
    for e in entries:
        pub, title = _published(e), clean_text(e.get("title"))
        if not (pub and title):
            continue
        source = (e.get("source") or {}).get("title") or ""
        if source and title.endswith(f" - {source}"):
            title = title[: -len(source) - 3]
        link = e.get("link", "")
        items.append(NewsItem(title=title, source=source, published=pub, url=link, provider="google", provider_id=link))
    return items


def google_query(ticker: Ticker, lookback_hours: float) -> str:
    terms = [f'"{n}"' for n in [ticker.name, *ticker.aliases] if n] + [ticker.symbol]
    days = max(1, math.ceil(lookback_hours / 24))
    return urllib.parse.quote(f"({' OR '.join(terms)}) when:{days}d")


# --- fetching ------------------------------------------------------------------


def _get_entries(url: str, timeout: float) -> list:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return feedparser.parse(r.content).entries


def fetch_raw(ticker: Ticker, cfg: NewsCfg) -> tuple[list[NewsItem], list[str]]:
    feeds = [
        ("Yahoo", YAHOO_RSS.format(symbol=urllib.parse.quote(ticker.symbol)), parse_yahoo),
        ("Google News", GOOGLE_RSS.format(query=google_query(ticker, cfg.lookback_hours)), parse_google),
    ]
    items, errors = [], []
    for name, url, parse in feeds:
        try:
            items += parse(_get_entries(url, cfg.timeout_s))
        except Exception as e:
            log.warning("%s: %s news feed failed: %s", ticker.symbol, name, e)
            errors.append(f"{name} news feed failed")
    return items, errors


# --- selection -----------------------------------------------------------------


def mention_matcher(ticker: Ticker):
    """True if a text mentions the company (names case-insensitive, symbol case-sensitive)."""
    names = [n for n in [ticker.name, *ticker.aliases] if n]
    name_re = re.compile(r"(?<!\w)(?:%s)(?!\w)" % "|".join(map(re.escape, names)), re.I) if names else None
    sym_re = re.compile(r"(?<!\w)\$?%s(?!\w)" % re.escape(ticker.symbol))
    return lambda text: bool(text) and bool(sym_re.search(text) or (name_re and name_re.search(text)))


# words that say nothing about which story a headline is about
STOPWORDS = frozenset("""
a an the and or but of in on at to for from by with as is are was were be been being it its this that these
those their his her they we you our your after amid over into out up down off about than then so not no new
says said say report reports update updates inc corp company stock stocks shares share why how what who when
where will would could can may might should more most just now today week year vs here heres
""".split())


def content_words(title: str, company_words: set[str]) -> set[str]:
    return {
        w for w in normalize_title(title).split()
        if len(w) > 2 and w not in STOPWORDS and w not in company_words
    }  # fmt: skip


def _trim(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    cut = text[:limit]
    end = cut.rfind(". ")
    if end > limit // 2:  # prefer a whole sentence when one fits
        return cut[: end + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:") + "…"


def select(
    items: list[NewsItem], ticker: Ticker, cfg: NewsCfg, now: datetime, seen: set[str]
) -> list[NewsItem]:
    """Filter, rank, group into stories, and keep the top story per group (newest first).

    Ranking: fresh first, then headline actually names the company, then
    preferred outlet, then recency. Two headlines are the same story when their
    titles are near-identical, or share enough content words (company name and
    filler words excluded). A story's snippet comes from any member that has
    one (Google items never do).
    """
    cutoff = now - timedelta(hours=cfg.lookback_hours)
    fresh_cutoff = now - timedelta(hours=cfg.fresh_hours)
    skip = [re.compile(p) for p in cfg.skip_title_patterns]
    if ticker.news_exclude:
        skip.append(re.compile("|".join(map(re.escape, ticker.news_exclude)), re.I))
    preferred = [s.lower() for s in cfg.preferred_sources]
    blocked = [s.lower() for s in cfg.blocked_sources]
    mentions = mention_matcher(ticker)

    ranked = []
    for it in items:
        if not cutoff <= it.published <= now + timedelta(hours=1):
            continue
        if any(p.search(it.title) for p in skip) or any(b in it.source.lower() for b in blocked):
            continue
        in_title = mentions(it.title)
        if not (in_title or mentions(it.snippet)):
            continue
        it = replace(it, fresh=it.published >= fresh_cutoff, new=it.key not in seen)
        is_pref = any(p in it.source.lower() for p in preferred)
        ranked.append(((it.fresh, in_title, is_pref, it.published), it))
    ranked.sort(key=lambda r: r[0], reverse=True)

    company = set(normalize_title(" ".join([ticker.name or "", *ticker.aliases, ticker.symbol])).split())
    kept = []
    for story in cluster_stories([it for _, it in ranked], company, cfg, max_stories=cfg.max_items):
        lead = story[0]
        lead.snippet = next((m.snippet for m in story if m.snippet), None)
        kept.append(lead)

    for it in kept:
        if it.snippet:
            it.snippet = _trim(it.snippet, cfg.snippet_chars)
    return sorted(kept, key=lambda i: i.published, reverse=True)


def cluster_stories(
    items: list[NewsItem], company_words: set[str], cfg: NewsCfg, max_stories: int | None = None,
    lead_only: bool = False,
) -> list[list[NewsItem]]:
    """Group items (best first) into stories; each story's first item is its lead.

    Two headlines are the same story when their titles are near-identical, or
    share enough content words (company names and filler words excluded), and
    they don't concern different watchlist companies. Once `max_stories` exist,
    later items only join existing stories. `lead_only` compares with each
    story's first item only; with hundreds of headlines, matching any member
    chains loosely similar headlines into one giant "story".
    """
    words = {id(it): content_words(it.title, company_words) for it in items}

    def same_story(a: NewsItem, b: NewsItem) -> bool:
        if a.tickers and b.tickers and not set(a.tickers) & set(b.tickers):
            return False  # "AAPL beats estimates" and "MSFT beats estimates" are different stories
        wa, wb = words[id(a)], words[id(b)]
        shared = len(wa & wb)
        if shared >= cfg.dedup_min_shared_words and shared / max(1, min(len(wa), len(wb))) >= cfg.dedup_word_overlap:
            return True
        sm = SequenceMatcher(None, a.key, b.key)  # cheap upper bounds first: digests compare hundreds of titles
        thr = cfg.dedup_similarity
        return sm.real_quick_ratio() >= thr and sm.quick_ratio() >= thr and sm.ratio() >= thr

    stories: list[list[NewsItem]] = []
    for it in items:
        story = next((st for st in stories if any(same_story(m, it) for m in (st[:1] if lead_only else st))), None)
        if story is not None:
            story.append(it)
        elif max_stories is None or len(stories) < max_stories:
            stories.append([it])
    return stories


def gather(
    tickers: list[Ticker], cfg: NewsCfg, conn: sqlite3.Connection, now: datetime | None = None
) -> dict[str, NewsResult]:
    now = now or datetime.now(timezone.utc)
    with ThreadPoolExecutor(max_workers=cfg.workers) as pool:
        raw = list(pool.map(lambda t: fetch_raw(t, cfg), tickers))
    out = {}
    for t, (items, errors) in zip(tickers, raw):
        seen = storage.seen_news_keys(conn, t.symbol)
        out[t.symbol] = NewsResult(select(items, t, cfg, now, seen), errors, n_raw=len(items))
    log.info(
        "news: %d raw items -> %d kept for %d tickers",
        sum(r.n_raw for r in out.values()),
        sum(len(r.items) for r in out.values()),
        len(out),
    )
    return out


def mark_seen(conn: sqlite3.Connection, results: dict[str, NewsResult]) -> None:
    """Record the shown headlines so later runs don't count them as new (real runs only)."""
    for symbol, res in results.items():
        storage.mark_news_seen(conn, symbol, [i.key for i in res.items])
