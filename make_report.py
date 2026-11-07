"""Draws the summary chart from the crawled database."""
import sqlite3
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS = Path(__file__).parent / "results"
db = sqlite3.connect(RESULTS / "catalogue.sqlite")
categories = db.execute("SELECT category, COUNT(*), AVG(price) FROM products WHERE category NOT IN ('Default','Add a comment') GROUP BY category ORDER BY COUNT(*) DESC LIMIT 12").fetchall()
prices = [r[0] for r in db.execute("SELECT price FROM products")]
ratings = dict(db.execute("SELECT rating, COUNT(*) FROM products GROUP BY rating").fetchall())

fig, (left, right) = plt.subplots(1, 2, figsize=(14, 5.6), dpi=110, gridspec_kw={"width_ratios": [1.15, 1]})
names = [c[0] for c in categories][::-1]
left.barh(names, [c[1] for c in categories][::-1], color="#0f6c74")
left.set_title("Books per category, largest 12", fontsize=15)
left.tick_params(labelsize=12)
left.grid(axis="x", alpha=0.3)
right.hist(prices, bins=25, color="#7b5ea7")
right.set_title(f"Price distribution, {len(prices):,} products", fontsize=15)
right.set_xlabel("Price (GBP)", fontsize=13)
right.tick_params(labelsize=12)
right.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(RESULTS / "report.png")
print(len(prices), ratings)
