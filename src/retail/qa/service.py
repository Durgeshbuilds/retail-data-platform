"""Answer a question end to end: parse -> resolve products -> template query -> sentence.

No language model is involved. Every answer carries how it was understood (the
period with exact dates, stores, matched and excluded products) and the SQL that
produced it, so a wrong interpretation is visible rather than silently wrong.
"""

from datetime import date, timedelta
from typing import Any

from pydantic import BaseModel, Field

from retail.dashboard.charts import inr
from retail.dashboard.data import store_today
from retail.qa.engine import QueryEngine, QueryResult
from retail.qa.parse import ParsedQuestion, StoreRef, parse
from retail.qa.periods import Period
from retail.search.resolve import ProductRef, ProductResolver, Resolution

SALES_INTENTS = ("sales", "top_products", "top_categories")
DEFAULT_DAYS = 30


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=300)
    # Set when the user un-ticks (or re-ticks) matched products; must be a subset of
    # the candidates the question's subject resolves to.
    product_ids: list[str] | None = None


class ProductOut(BaseModel):
    product_id: str
    name: str
    unit: str
    category: str
    brand: str
    score: float | None = None


class MatchOut(BaseModel):
    query: str
    match_type: str
    selected: list[ProductOut]
    excluded: list[ProductOut]
    note: str


class PeriodOut(BaseModel):
    start: date
    end: date
    label: str
    description: str


class AskResponse(BaseModel):
    question: str
    intent: str
    metric: str
    period: PeriodOut | None
    store_ids: list[str]
    subject: str | None
    match: MatchOut | None
    answer: str
    columns: list[str] = []
    rows: list[list[Any]] = []
    sql: str | None = None
    params: dict[str, Any] = {}
    notes: list[str] = []


def _product_out(p: ProductRef) -> ProductOut:
    return ProductOut(**p.__dict__)


def match_out(r: Resolution) -> MatchOut:
    return MatchOut(query=r.query, match_type=r.match_type, note=r.note,
                    selected=[_product_out(p) for p in r.selected],
                    excluded=[_product_out(p) for p in r.excluded])


def _apply_override(r: Resolution, product_ids: list[str]) -> Resolution:
    candidates = r.selected + r.excluded
    chosen = set(product_ids)
    return Resolution(r.query, r.match_type,
                      [p for p in candidates if p.product_id in chosen],
                      [p for p in candidates if p.product_id not in chosen],
                      note="Products adjusted by you.")


class QAService:
    def __init__(self, resolver: ProductResolver, engine: QueryEngine,
                 stores: list[StoreRef]) -> None:
        self.resolver, self.engine, self.stores = resolver, engine, stores

    def _store_phrase(self, store_ids: list[str]) -> str:
        if not store_ids or len(store_ids) == len(self.stores):
            return "all stores"
        names = {s.store_id: f"{s.name} ({s.store_id})" for s in self.stores}
        return ", ".join(names[s] for s in store_ids)

    def ask(self, question: str, product_ids: list[str] | None = None,
            today: date | None = None) -> AskResponse:
        today = today or store_today()
        q = parse(question, self.stores, today)
        notes: list[str] = []
        period = q.period
        if q.intent in SALES_INTENTS and period is None:
            period = Period(today - timedelta(days=DEFAULT_DAYS - 1), today,
                            f"Last {DEFAULT_DAYS} days")
            notes.append(f"No period in the question, so I used the last {DEFAULT_DAYS} days.")

        resolution = None
        if q.subject:
            resolution = self.resolver.resolve(q.subject)
            if product_ids is not None:
                resolution = _apply_override(resolution, product_ids)
        base = dict(
            question=question, intent=q.intent, metric=q.metric, store_ids=q.store_ids,
            subject=q.subject, match=match_out(resolution) if resolution else None,
            period=PeriodOut(start=period.start, end=period.end, label=period.label,
                             description=period.describe()) if period else None,
        )
        if resolution and not resolution.selected:
            return AskResponse(**base, answer=f"I couldn't find any products matching "
                                              f"“{q.subject}”.", notes=notes)
        if q.intent in ("stock", "price") and not resolution:
            return AskResponse(**base, answer="Which product? For example: “stock of Amul "
                                              "butter in Pune” or “price of toned milk”.")
        if resolution and resolution.note:
            notes.append(resolution.note)

        result = self.engine.run(
            q.intent, q.metric,
            start=period.start if period else None, end=period.end if period else None,
            store_ids=q.store_ids or [s.store_id for s in self.stores],
            product_ids=[p.product_id for p in resolution.selected] if resolution else None,
            limit=q.limit,
        )
        cutover = self.engine.cutover()
        if period and period.start < cutover <= period.end:
            notes.append(f"Days before {cutover:%d %b} come from the gold tables (last "
                         f"`make lake`); from {cutover:%d %b} on, from the live store.")
        return AskResponse(
            **base, answer=self._sentence(q, period, resolution, result),
            columns=result.columns, rows=[list(r) for r in result.rows],
            sql=result.sql, params=result.params, notes=notes,
        )

    def _sentence(self, q: ParsedQuestion, period: Period | None,
                  resolution: Resolution | None, result: QueryResult) -> str:
        where = self._store_phrase(q.store_ids)
        when = period.phrase() if period else ""
        what = f"“{q.subject}”" if q.subject else "all products"
        products = f" across {len(resolution.selected)} products" if resolution and len(
            resolution.selected) > 1 else ""
        rows = result.rows
        if q.intent == "sales":
            if not rows:
                return f"No sales of {what} at {where} {when}."
            units = sum(r[1] for r in rows)
            revenue = sum(r[2] for r in rows)
            if q.metric == "revenue":
                return (f"Revenue from {what} at {where} {when} was {inr(revenue)}, "
                        f"from {units:,} units{products}.")
            sold = f"{units:,} units of {what}{products}" if q.subject else f"{units:,} units"
            return (f"{where[0].upper() + where[1:]} sold {sold} {when}, "
                    f"worth {inr(revenue)}.")
        if q.intent in ("top_products", "top_categories"):
            if not rows:
                return f"No sales at {where} {when}."
            kind = "products" if q.intent == "top_products" else "categories"
            value = (lambda r: inr(r[-1])) if q.metric == "revenue" else (
                lambda r: f"{r[-2]:,} units")
            ranked = "; ".join(f"{i}. {r[0]} — {value(r)}" for i, r in enumerate(rows, 1))
            scope = f" among {what}" if q.subject else ""
            return f"Top {len(rows)} {kind}{scope} by {q.metric} at {where} {when}: {ranked}."
        if q.intent == "stock":
            if not rows:
                return f"No stock records for {what} at {where}."
            total = sum(r[2] for r in rows)
            low = [f"{r[1]} at {r[0]}" for r in rows if r[2] <= r[3]]
            flag = f" At or below reorder level: {', '.join(low)}." if low else ""
            return f"Current stock of {what} at {where}: {total:,} units{products}.{flag}"
        if not rows:
            return f"No price found for {what}."
        prices = "; ".join(f"{r[0]}: {inr(r[1]) if r[1] >= 1000 else f'₹{r[1]:.2f}'}"
                           for r in rows)
        return f"Current price of {what}: {prices}."
