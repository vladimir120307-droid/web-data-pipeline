"""Resumable web data pipeline: polite crawling, validation, change tracking, quality report.

It keeps its queue in SQLite, so a stopped run continues where it left off. It reads robots.txt,
limits its request rate, retries only what is worth retrying and re-fetches unchanged pages cheaply.
Pages that fail validation go to a rejects table with the reason instead of quietly polluting the data.
"""
from __future__ import annotations

import asyncio
import csv
import json
import random
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (url TEXT PRIMARY KEY, kind TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'todo');
CREATE TABLE IF NOT EXISTS cache (url TEXT PRIMARY KEY, etag TEXT, last_modified TEXT);
CREATE TABLE IF NOT EXISTS links (parent TEXT NOT NULL, url TEXT NOT NULL, kind TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS products (
    upc TEXT PRIMARY KEY, url TEXT NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL, price REAL NOT NULL,
    in_stock INTEGER NOT NULL, stock_count INTEGER, rating INTEGER, description TEXT, first_seen REAL, last_seen REAL);
CREATE TABLE IF NOT EXISTS changes (upc TEXT, field TEXT, old TEXT, new TEXT, seen_at REAL);
CREATE TABLE IF NOT EXISTS rejects (url TEXT PRIMARY KEY, reason TEXT, seen_at REAL);
"""
RATINGS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


class Product(BaseModel):
    upc: str = Field(min_length=1)
    url: str
    title: str = Field(min_length=1)
    category: str = Field(min_length=1)
    price: float = Field(gt=0, lt=100_000)
    in_stock: bool
    stock_count: int | None = Field(default=None, ge=0)
    rating: int | None = Field(default=None, ge=1, le=5)
    description: str | None = None


def parse_list(html: str, page_url: str) -> tuple[list[str], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    items = [urljoin(page_url, a["href"]) for a in soup.select("article.product_pod h3 a[href]")]
    nxt = soup.select_one("li.next a[href]")
    return items, urljoin(page_url, nxt["href"]) if nxt else None


def parse_product(html: str, url: str) -> Product:
    soup = BeautifulSoup(html, "html.parser")
    table = {row.th.get_text(strip=True): row.td.get_text(strip=True) for row in soup.select("table.table-striped tr") if row.th and row.td}
    title = soup.select_one("div.product_main h1")
    price = soup.select_one("div.product_main p.price_color")
    crumbs = soup.select("ul.breadcrumb li a")
    availability = soup.select_one("div.product_main p.availability")
    rating_tag = soup.select_one("div.product_main p.star-rating")
    description = soup.select_one("#product_description + p")
    price_match = re.search(r"\d+(?:\.\d+)?", price.get_text() if price else "")
    stock_text = availability.get_text(" ", strip=True) if availability else ""
    stock_match = re.search(r"\((\d+) available\)", stock_text)
    rating_word = next((c for c in (rating_tag.get("class", []) if rating_tag else []) if c in RATINGS), None)
    return Product(
        upc=table.get("UPC", ""), url=url, title=title.get_text(strip=True) if title else "",
        category=crumbs[-1].get_text(strip=True) if len(crumbs) >= 3 else "",
        price=float(price_match.group()) if price_match else 0.0, in_stock="In stock" in stock_text,
        stock_count=int(stock_match.group(1)) if stock_match else None,
        rating=RATINGS.get(rating_word) if rating_word else None,
        description=description.get_text(strip=True) if description else None,
    )


class RateLimiter:
    """Spaces requests evenly across all workers."""

    def __init__(self, per_second: float):
        self.interval = 1.0 / per_second
        self.next_slot = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            delay = self.next_slot - now
            self.next_slot = max(now, self.next_slot) + self.interval
        if delay > 0:
            await asyncio.sleep(delay)


@dataclass
class Stats:
    requests: int = 0
    not_modified: int = 0
    retries: int = 0
    failed: int = 0
    disallowed: int = 0
    products_new: int = 0
    products_updated: int = 0
    rejected: int = 0
    changes: int = 0
    statuses: dict = field(default_factory=dict)


class Pipeline:
    def __init__(self, start_url: str, db_path: str | Path, per_second: float = 6.0, concurrency: int = 6, max_retries: int = 4,
                 transport: httpx.AsyncBaseTransport | None = None, backoff_base: float = 0.5, max_pages: int | None = None):
        self.start_url = start_url
        self.db = sqlite3.connect(db_path)
        self.db.executescript(SCHEMA)
        self.limiter = RateLimiter(per_second)
        self.concurrency, self.max_retries, self.backoff_base = concurrency, max_retries, backoff_base
        self.transport = transport
        self.max_pages = max_pages
        self.stats = Stats()
        self.robots = RobotFileParser()
        self.user_agent = "PortfolioDemoCrawler/1.0"

    async def _load_robots(self, client: httpx.AsyncClient) -> None:
        parts = urlparse(self.start_url)
        try:
            r = await client.get(f"{parts.scheme}://{parts.netloc}/robots.txt")
            self.robots.parse(r.text.splitlines() if r.status_code == 200 else [])
        except httpx.HTTPError:
            self.robots.parse([])

    def _enqueue(self, url: str, kind: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO queue (url, kind) VALUES (?,?)", (url, kind))

    async def _get(self, client: httpx.AsyncClient, url: str) -> tuple[int, str | None]:
        cached = self.db.execute("SELECT etag, last_modified FROM cache WHERE url=?", (url,)).fetchone()
        headers = {}
        if cached:
            if cached[0]:
                headers["If-None-Match"] = cached[0]
            if cached[1]:
                headers["If-Modified-Since"] = cached[1]
        for attempt in range(self.max_retries + 1):
            await self.limiter.wait()
            self.stats.requests += 1
            try:
                r = await client.get(url, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError):
                status, retry_after = None, None
            else:
                status, retry_after = r.status_code, r.headers.get("Retry-After")
                self.stats.statuses[status] = self.stats.statuses.get(status, 0) + 1
                if status == 304:
                    self.stats.not_modified += 1
                    return 304, None
                if status == 200:
                    self.db.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?)", (url, r.headers.get("ETag"), r.headers.get("Last-Modified")))
                    return 200, r.content.decode("utf-8", "replace")
                if status not in RETRY_STATUS:
                    return status, None
            if attempt == self.max_retries:
                return status or 0, None
            self.stats.retries += 1
            wait = float(retry_after) if retry_after and retry_after.isdigit() else self.backoff_base * 2**attempt * random.uniform(0.8, 1.2)
            await asyncio.sleep(wait)
        return 0, None

    def _save(self, item: Product) -> None:
        now = time.time()
        row = self.db.execute("SELECT price, in_stock, stock_count, title FROM products WHERE upc=?", (item.upc,)).fetchone()
        values = (item.price, int(item.in_stock), item.stock_count, item.title)
        if row is None:
            self.stats.products_new += 1
            self.db.execute("INSERT INTO products VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            (item.upc, item.url, item.title, item.category, item.price, int(item.in_stock), item.stock_count, item.rating, item.description, now, now))
            return
        for name, old, new in zip(("price", "in_stock", "stock_count", "title"), row, values):
            if old != new:
                self.db.execute("INSERT INTO changes VALUES (?,?,?,?,?)", (item.upc, name, str(old), str(new), now))
                self.stats.changes += 1
        self.stats.products_updated += 1
        self.db.execute("UPDATE products SET url=?, title=?, category=?, price=?, in_stock=?, stock_count=?, rating=?, description=?, last_seen=? WHERE upc=?",
                        (item.url, item.title, item.category, item.price, int(item.in_stock), item.stock_count, item.rating, item.description, now, item.upc))

    async def _handle(self, client: httpx.AsyncClient, url: str, kind: str) -> None:
        if not self.robots.can_fetch(self.user_agent, url):
            self.stats.disallowed += 1
            self.db.execute("UPDATE queue SET state='skipped' WHERE url=?", (url,))
            return
        status, text = await self._get(client, url)
        if status == 304:
            # An unchanged list page still leads to its items, because any of those may have changed.
            for link, link_kind in self.db.execute("SELECT url, kind FROM links WHERE parent=?", (url,)).fetchall():
                self._enqueue(link, link_kind)
            self.db.execute("UPDATE queue SET state='done' WHERE url=?", (url,))
            return
        if status != 200 or text is None:
            self.stats.failed += 1
            self.db.execute("UPDATE queue SET state='failed' WHERE url=?", (url,))
            return
        if kind == "list":
            items, nxt = parse_list(text, url)
            found = [(link, "item") for link in items] + ([(nxt, "list")] if nxt else [])
            self.db.execute("DELETE FROM links WHERE parent=?", (url,))
            for link, link_kind in found:
                self.db.execute("INSERT INTO links VALUES (?,?,?)", (url, link, link_kind))
                self._enqueue(link, link_kind)
        else:
            try:
                self._save(parse_product(text, url))
            except ValidationError as exc:
                first = exc.errors()[0]
                self.db.execute("INSERT OR REPLACE INTO rejects VALUES (?,?,?)", (url, f"{first['loc'][0]}: {first['msg']}", time.time()))
                self.stats.rejected += 1
        self.db.execute("UPDATE queue SET state='done' WHERE url=?", (url,))

    async def run(self, resume: bool = True) -> Stats:
        if not resume:
            self.db.execute("DELETE FROM queue")
        self._enqueue(self.start_url, "list")
        self.db.commit()
        limits = httpx.Limits(max_connections=self.concurrency)
        async with httpx.AsyncClient(timeout=30, transport=self.transport, limits=limits, follow_redirects=True,
                                     headers={"User-Agent": self.user_agent}) as client:
            await self._load_robots(client)
            handled, active = 0, set()
            while True:
                room = self.concurrency - len(active)
                rows = self.db.execute("SELECT url, kind FROM queue WHERE state='todo' ORDER BY kind='item' DESC LIMIT ?", (room,)).fetchall() if room > 0 else []
                if self.max_pages is not None:
                    rows = rows[: max(0, self.max_pages - handled)]
                for url, kind in rows:
                    self.db.execute("UPDATE queue SET state='doing' WHERE url=?", (url,))
                    active.add(asyncio.create_task(self._handle(client, url, kind)))
                    handled += 1
                if not active:
                    break
                done, active = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
                self.db.commit()
        self.db.execute("UPDATE queue SET state='todo' WHERE state='doing'")
        self.db.commit()
        return self.stats

    # ------------------------------------------------------------ outputs
    def export_csv(self, path: str | Path) -> int:
        rows = self.db.execute("SELECT upc, title, category, price, in_stock, stock_count, rating, url FROM products ORDER BY category, title").fetchall()
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["upc", "title", "category", "price", "in_stock", "stock_count", "rating", "url"])
            writer.writerows(rows)
        return len(rows)

    def quality_report(self) -> dict:
        q = self.db.execute
        total = q("SELECT COUNT(*) FROM products").fetchone()[0]
        return {
            "products": total,
            "categories": q("SELECT COUNT(DISTINCT category) FROM products").fetchone()[0],
            "missing_description": q("SELECT COUNT(*) FROM products WHERE description IS NULL OR description=''").fetchone()[0],
            "missing_rating": q("SELECT COUNT(*) FROM products WHERE rating IS NULL").fetchone()[0],
            "duplicate_titles": q("SELECT COUNT(*) FROM (SELECT title FROM products GROUP BY title HAVING COUNT(*)>1)").fetchone()[0],
            "price_min": q("SELECT MIN(price) FROM products").fetchone()[0],
            "price_max": q("SELECT MAX(price) FROM products").fetchone()[0],
            "rejected_pages": q("SELECT COUNT(*) FROM rejects").fetchone()[0],
            "failed_pages": q("SELECT COUNT(*) FROM queue WHERE state='failed'").fetchone()[0],
        }


def stats_dict(stats: Stats) -> dict:
    return {k: (dict(v) if isinstance(v, dict) else v) for k, v in vars(stats).items()}


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Crawl a catalogue into SQLite and CSV")
    ap.add_argument("start_url")
    ap.add_argument("--db", default="catalogue.sqlite")
    ap.add_argument("--rate", type=float, default=6.0)
    ap.add_argument("--fresh", action="store_true", help="restart the queue instead of resuming")
    args = ap.parse_args()
    pipe = Pipeline(args.start_url, args.db, per_second=args.rate)
    started = time.time()
    stats = asyncio.run(pipe.run(resume=not args.fresh))
    pipe.export_csv(Path(args.db).with_suffix(".csv"))
    print(json.dumps({"seconds": round(time.time() - started, 1), **stats_dict(stats), **pipe.quality_report()}, indent=2))


if __name__ == "__main__":
    main()
