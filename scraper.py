"""
async-scraper-mongodb
=====================
Async scraper pipeline: asyncio + aiohttp → BeautifulSoup → MongoDB Atlas (motor)
Includes sync baseline for speed comparison.

Install:
    pip install aiohttp motor pymongo beautifulsoup4 lxml python-dotenv

Setup:
    Copy .env.example to .env and fill in your MongoDB Atlas URI.
    Get free cluster at https://www.mongodb.com/cloud/atlas/register
"""

import asyncio
import time
import re
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import requests
from bs4 import BeautifulSoup
import motor.motor_asyncio
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "scraper_db"
COLLECTION_NAME = "articles"

# Limit concurrent requests — be a polite scraper
MAX_CONCURRENT = 10

# Test URLs (publicly scrapable, no auth required)
TEST_URLS = [
    "https://realpython.com/async-io-python/",
    "https://realpython.com/python-requests/",
    "https://realpython.com/python-f-strings/",
    "https://realpython.com/python-type-checking/",
    "https://realpython.com/python-kwargs-and-args/",
    "https://realpython.com/python-exceptions/",
    "https://realpython.com/python-lambda/",
    "https://realpython.com/python-lists-tuples/",
    "https://realpython.com/python-dicts/",
    "https://realpython.com/python-sets/",
]


# ─── Data Model ────────────────────────────────────────────────────────────────

@dataclass
class ScrapedArticle:
    url: str
    title: Optional[str]
    description: Optional[str]
    word_count: int
    links: list[str]
    scraped_at: datetime
    status_code: int
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["scraped_at"] = self.scraped_at.isoformat()
        return d


# ─── HTML Parsing ──────────────────────────────────────────────────────────────

def parse_html(url: str, html: str, status_code: int) -> ScrapedArticle:
    """Parse raw HTML into a structured ScrapedArticle. Pure CPU — no I/O."""
    soup = BeautifulSoup(html, "lxml")

    title = None
    if soup.title:
        title = soup.title.get_text(strip=True)

    # Meta description
    description = None
    meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if meta:
        description = meta.get("content", "").strip()

    # Count words in main body text
    body_text = soup.get_text(separator=" ", strip=True)
    word_count = len(body_text.split())

    # Extract all links
    links = [
        a["href"] for a in soup.find_all("a", href=True)
        if a["href"].startswith("http")
    ][:50]  # cap at 50 links per page

    return ScrapedArticle(
        url=url,
        title=title,
        description=description,
        word_count=word_count,
        links=links,
        scraped_at=datetime.now(timezone.utc),
        status_code=status_code,
    )


# ─── Async Scraper ─────────────────────────────────────────────────────────────

async def fetch_and_parse(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> ScrapedArticle:
    """
    Fetch one URL asynchronously, parse it, return a ScrapedArticle.
    Semaphore limits max concurrent requests to MAX_CONCURRENT.
    """
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                html = await resp.text()
                return parse_html(url, html, resp.status)
        except asyncio.TimeoutError:
            return ScrapedArticle(
                url=url, title=None, description=None, word_count=0,
                links=[], scraped_at=datetime.now(timezone.utc),
                status_code=0, error="Timeout"
            )
        except Exception as e:
            return ScrapedArticle(
                url=url, title=None, description=None, word_count=0,
                links=[], scraped_at=datetime.now(timezone.utc),
                status_code=0, error=str(e)
            )


async def scrape_async(urls: list[str]) -> tuple[list[ScrapedArticle], float]:
    """
    Scrape all URLs concurrently using asyncio + aiohttp.
    Returns (results, elapsed_seconds).
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    start = time.perf_counter()

    # One shared session for connection pooling (faster than opening per request)
    headers = {"User-Agent": "AsyncScraper/1.0 (educational project)"}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_and_parse(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)  # run all concurrently

    elapsed = time.perf_counter() - start
    return list(results), elapsed


# ─── Sync Scraper (baseline for comparison) ────────────────────────────────────

def scrape_sync(urls: list[str]) -> tuple[list[ScrapedArticle], float]:
    """
    Scrape all URLs one by one using requests (blocking/synchronous).
    Returns (results, elapsed_seconds). Used for speed comparison.
    """
    headers = {"User-Agent": "SyncScraper/1.0 (educational project)"}
    results = []
    start = time.perf_counter()

    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            article = parse_html(url, resp.text, resp.status_code)
            results.append(article)
            print(f"  [sync] fetched: {url[:60]}")
        except Exception as e:
            results.append(ScrapedArticle(
                url=url, title=None, description=None, word_count=0,
                links=[], scraped_at=datetime.now(timezone.utc),
                status_code=0, error=str(e)
            ))

    elapsed = time.perf_counter() - start
    return results, elapsed


# ─── MongoDB: Save Results ──────────────────────────────────────────────────────

async def save_to_mongodb(articles: list[ScrapedArticle]) -> int:
    """
    Save scraped articles to MongoDB Atlas using motor (async driver).
    Uses upsert on URL to avoid duplicates if you run the scraper twice.
    Returns count of inserted/updated documents.
    """
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Create a unique index on URL (idempotent — safe to call repeatedly)
    await collection.create_index("url", unique=True)

    saved = 0
    for article in articles:
        if article.error:
            continue  # skip failed fetches

        try:
            await collection.update_one(
                {"url": article.url},               # match by URL
                {"$set": article.to_dict()},        # update or create
                upsert=True                         # insert if not found
            )
            saved += 1
        except Exception as e:
            print(f"  [mongo] failed to save {article.url}: {e}")

    client.close()
    return saved


async def fetch_from_mongodb(limit: int = 5) -> list[dict]:
    """Read back documents from MongoDB to verify they were saved."""
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Find docs, sort by scraped_at descending, project only key fields
    cursor = collection.find(
        {},
        {"url": 1, "title": 1, "word_count": 1, "scraped_at": 1, "_id": 0}
    ).sort("scraped_at", -1).limit(limit)

    docs = await cursor.to_list(length=limit)
    client.close()
    return docs


# ─── Aggregation Example ───────────────────────────────────────────────────────

async def get_stats() -> dict:
    """
    MongoDB aggregation pipeline: compute stats across all scraped articles.
    This shows MongoDB's power — run analytics directly in the database.
    """
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {"$match": {"error": None}},                     # only successful scrapes
        {"$group": {
            "_id": None,
            "total_articles": {"$sum": 1},
            "avg_word_count": {"$avg": "$word_count"},
            "max_word_count": {"$max": "$word_count"},
            "total_links": {"$sum": {"$size": "$links"}},
        }},
        {"$project": {
            "_id": 0,
            "total_articles": 1,
            "avg_word_count": {"$round": ["$avg_word_count", 0]},
            "max_word_count": 1,
            "total_links": 1,
        }}
    ]

    result = await collection.aggregate(pipeline).to_list(length=1)
    client.close()
    return result[0] if result else {}


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    urls = TEST_URLS
    print(f"\n{'='*60}")
    print(f"  Async Scraper Pipeline — {len(urls)} URLs")
    print(f"{'='*60}\n")

    # ── SYNC baseline ──────────────────────────────────────────
    print("Running SYNC scraper (sequential, blocking)...")
    sync_results, sync_time = scrape_sync(urls)
    print(f"  Sync done in {sync_time:.2f}s\n")

    # ── ASYNC scraper ──────────────────────────────────────────
    print("Running ASYNC scraper (concurrent, non-blocking)...")
    async_results, async_time = await scrape_async(urls)
    for r in async_results:
        status = f" {r.status_code}" if not r.error else f"✗ {r.error}"
        print(f"  [{status}] {r.url[:60]}")
    print(f"  Async done in {async_time:.2f}s\n")

    # ── Speed comparison ───────────────────────────────────────
    speedup = sync_time / async_time if async_time > 0 else 0
    print(f"{'─'*60}")
    print(f"     Speed comparison:")
    print(f"     Sync:  {sync_time:.2f}s")
    print(f"     Async: {async_time:.2f}s")
    print(f"     Speedup: {speedup:.1f}×  (async is {speedup:.1f}x faster)")
    print(f"{'─'*60}\n")

    # ── Save to MongoDB ────────────────────────────────────────
    print(" Saving results to MongoDB Atlas (via motor)...")
    saved = await save_to_mongodb(async_results)
    print(f"   Saved {saved} articles to '{DB_NAME}.{COLLECTION_NAME}'\n")

    # ── Read back ──────────────────────────────────────────────
    print(" Reading back from MongoDB...")
    docs = await fetch_from_mongodb(limit=3)
    for doc in docs:
        print(f"  • {doc.get('title', 'N/A')[:50]}  ({doc.get('word_count', 0)} words)")
    print()

    # ── Aggregation stats ──────────────────────────────────────
    print(" Running aggregation pipeline...")
    stats = await get_stats()
    if stats:
        print(f"  Total articles : {stats.get('total_articles')}")
        print(f"  Avg word count : {stats.get('avg_word_count')}")
        print(f"  Max word count : {stats.get('max_word_count')}")
        print(f"  Total links    : {stats.get('total_links')}")
    print(f"\n{'='*60}\n  Done!\n{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
