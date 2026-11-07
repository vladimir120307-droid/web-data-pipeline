import asyncio
import time

import httpx
import pytest

from pipeline import Pipeline, Product, RateLimiter, parse_list, parse_product

BASE = "http://shop.test/"


def product_page(upc="abc123", price="51.77", title="A Book", stock="In stock (22 available)", rating="Three", category="Travel"):
    return f"""<html><body><ul class="breadcrumb"><li><a href="/">Home</a></li><li><a href="/books">Books</a></li><li><a href="/c">{category}</a></li><li>{title}</li></ul>
    <div class="product_main"><h1>{title}</h1><p class="price_color">Â£{price}</p><p class="instock availability">{stock}</p><p class="star-rating {rating}"></p></div>
    <div id="product_description"><h2>Product Description</h2></div><p>Some text.</p>
    <table class="table table-striped"><tr><th>UPC</th><td>{upc}</td></tr></table></body></html>"""


def list_page(links, nxt=None):
    items = "".join(f'<article class="product_pod"><h3><a href="{l}">x</a></h3></article>' for l in links)
    more = f'<li class="next"><a href="{nxt}">next</a></li>' if nxt else ""
    return f"<html><body>{items}<ul>{more}</ul></body></html>"


class Site:
    """A fake website that misbehaves on request."""

    def __init__(self):
        self.pages = {"/robots.txt": "User-agent: *\nDisallow: /private/\n"}
        self.fail_times = {}
        self.calls = {}
        self.etags = {}
        self.retry_after = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls[path] = self.calls.get(path, 0) + 1
        if self.fail_times.get(path, 0) > 0:
            self.fail_times[path] -= 1
            headers = {"Retry-After": "0"} if path in self.retry_after else {}
            return httpx.Response(429 if path in self.retry_after else 503, headers=headers)
        if path not in self.pages:
            return httpx.Response(404)
        etag = self.etags.get(path)
        if etag and request.headers.get("If-None-Match") == etag:
            return httpx.Response(304)
        return httpx.Response(200, content=self.pages[path].encode("utf-8"), headers={"ETag": etag} if etag else {})


def run(site, db, **kw):
    pipe = Pipeline(BASE + "list1", db, per_second=2000, backoff_base=0.001, transport=httpx.MockTransport(site), **kw)
    stats = asyncio.run(pipe.run())
    return pipe, stats


@pytest.fixture()
def site():
    s = Site()
    s.pages["/list1"] = list_page(["/a", "/b"], "/list2")
    s.pages["/list2"] = list_page(["/c"])
    s.pages["/a"] = product_page("u-a", "10.00")
    s.pages["/b"] = product_page("u-b", "20.50", title="B Book", rating="Five")
    s.pages["/c"] = product_page("u-c", "5.25", title="C Book", stock="Out of stock", category="Poetry")
    return s


def test_parsers_read_real_page_structure_including_a_bad_encoding_on_the_price():
    p = parse_product(product_page(), BASE + "a")
    assert p == Product(upc="abc123", url=BASE + "a", title="A Book", category="Travel", price=51.77, in_stock=True,
                        stock_count=22, rating=3, description="Some text.")
    links, nxt = parse_list(list_page(["x.html"], "page-2.html"), "http://s.test/catalogue/page-1.html")
    assert links == ["http://s.test/catalogue/x.html"] and nxt == "http://s.test/catalogue/page-2.html"


def test_follows_pagination_and_stores_everything(site, tmp_path):
    pipe, stats = run(site, tmp_path / "t.db")
    assert stats.products_new == 3
    assert pipe.quality_report()["products"] == 3
    assert pipe.quality_report()["categories"] == 2


def test_temporary_errors_are_retried(site, tmp_path):
    site.fail_times["/a"] = 3
    pipe, stats = run(site, tmp_path / "t.db")
    assert stats.retries == 3
    assert stats.products_new == 3
    assert site.calls["/a"] == 4


def test_rate_limit_response_is_honoured(site, tmp_path):
    site.fail_times["/a"] = 1
    site.retry_after["/a"] = True
    _, stats = run(site, tmp_path / "t.db")
    assert stats.statuses[429] == 1 and stats.products_new == 3


def test_page_that_keeps_failing_is_marked_and_the_run_continues(site, tmp_path):
    site.fail_times["/a"] = 99
    pipe, stats = run(site, tmp_path / "t.db", max_retries=2)
    assert site.calls["/a"] == 3
    assert stats.failed == 1 and stats.products_new == 2
    assert pipe.quality_report()["failed_pages"] == 1


def test_missing_page_is_not_retried(site, tmp_path):
    site.pages["/list2"] = list_page(["/gone"])
    _, stats = run(site, tmp_path / "t.db")
    assert site.calls["/gone"] == 1 and stats.failed == 1


def test_robots_txt_is_respected(site, tmp_path):
    site.pages["/list1"] = list_page(["/a", "/private/secret"])
    site.pages["/private/secret"] = product_page("u-x")
    _, stats = run(site, tmp_path / "t.db")
    assert stats.disallowed == 1
    assert "/private/secret" not in site.calls


def test_broken_product_goes_to_rejects_not_into_the_data(site, tmp_path):
    site.pages["/b"] = product_page("u-b", price="0")
    pipe, stats = run(site, tmp_path / "t.db")
    assert stats.rejected == 1 and stats.products_new == 2
    reason = pipe.db.execute("SELECT reason FROM rejects").fetchone()[0]
    assert reason.startswith("price")


def test_second_run_records_a_price_change(site, tmp_path):
    db = tmp_path / "t.db"
    run(site, db)
    site.pages["/a"] = product_page("u-a", "12.00")
    pipe = Pipeline(BASE + "list1", db, per_second=2000, transport=httpx.MockTransport(site))
    stats = asyncio.run(pipe.run(resume=False))
    assert stats.changes == 1 and stats.products_new == 0
    assert pipe.db.execute("SELECT field, old, new FROM changes").fetchone() == ("price", "10.0", "12.0")
    assert pipe.db.execute("SELECT price FROM products WHERE upc='u-a'").fetchone()[0] == 12.0


def test_unchanged_pages_are_not_downloaded_again(site, tmp_path):
    site.etags["/a"] = '"v1"'
    db = tmp_path / "t.db"
    run(site, db)
    pipe = Pipeline(BASE + "list1", db, per_second=2000, transport=httpx.MockTransport(site))
    stats = asyncio.run(pipe.run(resume=False))
    assert stats.not_modified == 1


def test_unchanged_list_page_still_reaches_a_changed_item_behind_it(site, tmp_path):
    site.etags.update({"/list1": '"l1"', "/list2": '"l2"', "/a": '"a1"', "/b": '"b1"', "/c": '"c1"'})
    db = tmp_path / "t.db"
    run(site, db)
    site.pages["/c"] = product_page("u-c", "9.99", title="C Book", stock="Out of stock", category="Poetry")
    site.etags["/c"] = '"c2"'
    pipe = Pipeline(BASE + "list1", db, per_second=2000, transport=httpx.MockTransport(site))
    stats = asyncio.run(pipe.run(resume=False))
    assert stats.not_modified == 4  # both list pages and two unchanged items
    assert stats.changes == 1
    assert pipe.db.execute("SELECT price FROM products WHERE upc='u-c'").fetchone()[0] == 9.99


def test_stopped_run_resumes_without_repeating_work(site, tmp_path):
    db = tmp_path / "t.db"
    pipe, stats = run(site, db, max_pages=2)
    assert stats.products_new < 3
    before = dict(site.calls)
    pipe, stats = run(site, db)
    assert pipe.quality_report()["products"] == 3
    assert site.calls["/list1"] == before["/list1"]


def test_rate_limiter_spaces_requests():
    async def go():
        limiter = RateLimiter(100)
        started = time.monotonic()
        for _ in range(11):
            await limiter.wait()
        return time.monotonic() - started
    assert asyncio.run(go()) >= 0.09


def test_csv_export(site, tmp_path):
    pipe, _ = run(site, tmp_path / "t.db")
    out = tmp_path / "x.csv"
    assert pipe.export_csv(out) == 3
    assert out.read_text(encoding="utf-8").splitlines()[0].startswith("upc,title")
