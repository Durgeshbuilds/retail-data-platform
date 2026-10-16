from datetime import date

import pytest

from retail.qa.parse import StoreRef, parse
from retail.qa.periods import resolve_period

TODAY = date(2026, 10, 16)  # a Friday in Q4
STORES = [StoreRef("BLR01", "Koramangala", "Bengaluru"), StoreRef("PUN01", "Baner", "Pune"),
          StoreRef("HYD01", "Gachibowli", "Hyderabad")]


@pytest.mark.parametrize(("phrase", "start", "end", "label"), [
    ("today", date(2026, 10, 16), date(2026, 10, 16), "Today"),
    ("yesterday", date(2026, 10, 15), date(2026, 10, 15), "Yesterday"),
    ("this week", date(2026, 10, 12), date(2026, 10, 16), "This week"),
    ("last week", date(2026, 10, 5), date(2026, 10, 11), "Last week"),
    ("this month", date(2026, 10, 1), date(2026, 10, 16), "October 2026 to date"),
    ("last month", date(2026, 9, 1), date(2026, 9, 30), "September 2026"),
    ("last quarter", date(2026, 7, 1), date(2026, 9, 30), "Q3 2026"),
    ("this quarter", date(2026, 10, 1), date(2026, 10, 16), "Q4 2026 to date"),
    ("in Q1", date(2026, 1, 1), date(2026, 3, 31), "Q1 2026"),
    ("q4 2025", date(2025, 10, 1), date(2025, 12, 31), "Q4 2025"),
    ("last year", date(2025, 1, 1), date(2025, 12, 31), "2025"),
    ("this year", date(2026, 1, 1), date(2026, 10, 16), "2026 to date"),
    ("last 30 days", date(2026, 9, 17), date(2026, 10, 16), "Last 30 days"),
    ("past two weeks", date(2026, 10, 3), date(2026, 10, 16), "Last 2 weeks"),
    ("last 3 months", date(2026, 7, 17), date(2026, 10, 16), "Last 3 months"),
    ("in August", date(2026, 8, 1), date(2026, 8, 31), "August 2026"),
    ("in December", date(2025, 12, 1), date(2025, 12, 31), "December 2025"),  # most recent
    ("in oct", date(2026, 10, 1), date(2026, 10, 16), "October 2026 to date"),
    ("May 2026", date(2026, 5, 1), date(2026, 5, 31), "May 2026"),
])
def test_periods(phrase, start, end, label):
    period, rest = resolve_period(f"units sold {phrase} please", TODAY)
    assert (period.start, period.end, period.label) == (start, end, label)
    assert phrase.split()[-1].lower() not in rest.lower().split()


def test_period_edge_cases():
    assert resolve_period("what may we sell", TODAY)[0] is None  # "may" is a word, not a month
    assert resolve_period("sales in Q2 2030", TODAY)[0] is None  # future
    end_of_march = resolve_period("last month", date(2026, 3, 31))[0]
    assert (end_of_march.start, end_of_march.end) == (date(2026, 2, 1), date(2026, 2, 28))
    window = resolve_period("last 1 month", date(2026, 3, 31))[0]
    assert window.start == date(2026, 3, 1)
    assert resolve_period("last quarter", date(2026, 2, 1))[0].label == "Q4 2025"


@pytest.mark.parametrize(("question", "intent", "metric", "subject", "stores", "label"), [
    ("How many milks were sold last quarter?", "sales", "units", "milks", [], "Q3 2026"),
    ("Revenue from rice in Pune last month", "sales", "revenue", "rice", ["PUN01"],
     "September 2026"),
    ("How much atta did Koramangala sell this week?", "sales", "units", "atta", ["BLR01"],
     "This week"),
    ("Top 3 products at Gachibowli yesterday", "top_products", "units", None, ["HYD01"],
     "Yesterday"),
    ("best selling categories by revenue in August", "top_categories", "revenue", None, [],
     "August 2026"),
    ("How much Amul butter is left in Bengaluru?", "stock", "units", "amul butter", ["BLR01"],
     None),
    ("What is the price of toned milk?", "price", "units", "toned milk", [], None),
    ("total sales today", "sales", "units", None, [], "Today"),
])
def test_parse(question, intent, metric, subject, stores, label):
    q = parse(question, STORES, TODAY)
    assert (q.intent, q.metric, q.subject, q.store_ids) == (intent, metric, subject, stores)
    assert (q.period.label if q.period else None) == label


def test_top_n_limit():
    assert parse("top five products last week", STORES, TODAY).limit == 5
    assert parse("top 10 products last week", STORES, TODAY).limit == 10
    assert parse("best sellers", STORES, TODAY).limit == 5
