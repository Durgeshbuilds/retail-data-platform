"""Rule-based question parsing: what is asked, about what, where and when.

Deliberately simple and predictable. Anything it can't place becomes the
"subject" (e.g. "milks"), which product search then resolves to products.
"""

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from retail.qa.periods import NUMBER_WORDS, Period, resolve_period

Intent = Literal["sales", "top_products", "top_categories", "stock", "price"]
Metric = Literal["units", "revenue"]


@dataclass(frozen=True)
class StoreRef:
    store_id: str
    name: str
    city: str


@dataclass(frozen=True)
class ParsedQuestion:
    question: str
    intent: Intent
    metric: Metric
    period: Period | None
    store_ids: list[str] = field(default_factory=list)  # empty = all stores
    subject: str | None = None
    limit: int = 5


TOP = re.compile(
    r"\btop\s*(\d+|" + "|".join(NUMBER_WORDS) + r")?\b|\bbest[- ]?sell\w*|\bmost (?:popular|sold)\b"
    r"|\bhighest\b|\bbiggest\b",
    re.IGNORECASE,
)
STOCK = re.compile(r"\bstock\b|\binventory\b|\bleft\b|\bon hand\b|\bremaining\b", re.IGNORECASE)
PRICE = re.compile(r"\bprice[sd]?\b|\bcosts?\b", re.IGNORECASE)
REVENUE = re.compile(
    r"\brevenue\b|\bworth\b|\brupees?\b|₹|\bmoney\b|\bearn\w*|\bturnover\b|\bsales value\b"
    r"|\bin sales\b|\bvalue\b",
    re.IGNORECASE,
)
# Words that frame a question rather than name what it's about.
STOP_WORDS = set("""
    a an the of in at for from on by to per and or with during
    how many much what which who whose show me give tell list find get please
    is are was were be been do does did have has had there it its this that these those
    we our us you your i my
    sell sells sold selling sale sales unit units quantity number count total amount
    revenue worth rupee rupees money earn earned earning turnover value
    top best bestselling best-selling most popular highest biggest
    product products item items thing things category categories
    stock inventory left remaining on hand current currently now available
    price prices priced cost costs
    store stores shop shops outlet outlets branch all every each across overall
""".split())


def _find_stores(text: str, stores: list[StoreRef]) -> tuple[list[str], str]:
    found = []
    for s in stores:
        for alias in (s.store_id, s.name, s.city):
            pattern = re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
            if pattern.search(text):
                found.append(s.store_id)
                text = pattern.sub(" ", text)
    return sorted(set(found)), text


def _limit(match: re.Match | None) -> int:
    if match and match.group(1):
        word = match.group(1).lower()
        return int(word) if word.isdigit() else NUMBER_WORDS[word]
    return 5


def parse(question: str, stores: list[StoreRef], today: date) -> ParsedQuestion:
    text = question.strip()
    period, text = resolve_period(text, today)
    store_ids, text = _find_stores(text, stores)

    top = TOP.search(text)
    if top:
        intent: Intent = "top_categories" if re.search(r"categor", text, re.I) else "top_products"
    elif STOCK.search(text):
        intent = "stock"
    elif PRICE.search(text) and not re.search(r"\bsold\b|\bsell", text, re.I):
        intent = "price"
    else:
        intent = "sales"
    metric: Metric = "revenue" if REVENUE.search(text) else "units"

    text = TOP.sub(" ", text)
    words = re.findall(r"[a-z0-9+&'-]+", text.lower())
    subject = " ".join(w for w in words if w not in STOP_WORDS and not w.isdigit()) or None
    return ParsedQuestion(question, intent, metric, period, store_ids, subject, _limit(top))
