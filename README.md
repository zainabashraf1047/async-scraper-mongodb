# async-scraper-mongodb

An async web scraper pipeline using `asyncio + aiohttp` → `BeautifulSoup` → `MongoDB Atlas` via `motor`.

## Features

- **Concurrent fetching** with `asyncio.gather()` and `aiohttp.ClientSession`
- **Rate limiting** via `asyncio.Semaphore` (polite to servers)
- **HTML parsing** with BeautifulSoup — extracts title, description, word count, links
- **Async MongoDB** writes with `motor` (non-blocking, integrated with asyncio)
- **Upsert pattern** — safe to run multiple times without duplicate documents
- **Aggregation pipeline** — compute stats across all scraped articles
- **Sync vs async benchmark** — prints speedup factor

## Setup

```bash
# 1. Clone and enter directory
git clone https://github.com/YOU/async-scraper-mongodb
cd async-scraper-mongodb

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure MongoDB
cp .env.example .env
# Edit .env and paste your MongoDB Atlas connection string
# Get free cluster: https://www.mongodb.com/cloud/atlas/register

# 4. Run
python scraper.py
```

## Expected Output

```
============================================================
  Async Scraper Pipeline — 10 URLs
============================================================

▶ Running SYNC scraper (sequential, blocking)...
  [sync] fetched: https://realpython.com/async-io-python/
  ...
  ✓ Sync done in 48.32s

▶ Running ASYNC scraper (concurrent, non-blocking)...
  [✓ 200] https://realpython.com/async-io-python/
  ...
  ✓ Async done in 3.71s

────────────────────────────────────────────────────────────
  ⚡ Speed comparison:
     Sync:  48.32s
     Async: 3.71s
     Speedup: 13.0×  (async is 13.0x faster)
────────────────────────────────────────────────────────────

▶ Saving results to MongoDB Atlas (via motor)...
  ✓ Saved 10 articles to 'scraper_db.articles'

▶ Running aggregation pipeline...
  Total articles : 10
  Avg word count : 2840
  Max word count : 5200
  Total links    : 420
```

## Project Structure

```
async-scraper-mongodb/
├── scraper.py          # Main pipeline
├── requirements.txt
├── .env.example
└── README.md
```

## Key Concepts

| Concept | Used for |
|---|---|
| `asyncio.gather()` | Run all URL fetches concurrently |
| `asyncio.Semaphore` | Cap max concurrent requests (rate limiting) |
| `aiohttp.ClientSession` | Non-blocking HTTP with connection pooling |
| `async with` | Async context manager — ensures cleanup |
| `motor.AsyncIOMotorClient` | Async MongoDB driver |
| `collection.update_one(upsert=True)` | Insert-or-update pattern |
| MongoDB aggregation pipeline | Server-side analytics query |
