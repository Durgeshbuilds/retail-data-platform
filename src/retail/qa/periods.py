"""Turn time phrases into exact date ranges, in code (never guessed by a model).

Quarters are calendar quarters (Q3 = July-September), not the Indian financial
year, and the label always says which dates were used.
"""

import calendar
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from retail.catalog import add_months


@dataclass(frozen=True)
class Period:
    start: date
    end: date  # inclusive
    label: str

    def dates(self) -> str:
        """"16 Oct 2026", "1 Jul – 30 Sep 2026" or "1 Dec 2025 – 31 Jan 2026"."""
        if self.start == self.end:
            return _fmt(self.end)
        if self.start.year == self.end.year:
            return f"{self.start.day} {self.start:%b} – {_fmt(self.end)}"
        return f"{_fmt(self.start)} – {_fmt(self.end)}"

    def describe(self) -> str:
        return f"{self.label} ({self.dates()})"

    def phrase(self) -> str:
        """For use mid-sentence: "today (16 Oct 2026)", "in Q3 2026 (1 Jul – 30 Sep 2026)"."""
        relative = {"Today": "today", "Yesterday": "yesterday", "This week": "this week",
                    "Last week": "last week"}
        if self.label in relative:
            return f"{relative[self.label]} ({self.dates()})"
        if self.label.startswith("Last "):
            return f"in the {self.label.lower()} ({self.dates()})"
        return f"in {self.label} ({self.dates()})"


def _fmt(d: date) -> str:
    return f"{d.day} {d:%b %Y}"


def _month(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def _quarter(year: int, q: int) -> tuple[date, date]:
    start = date(year, 3 * q - 2, 1)
    return start, add_months(start, 3) - timedelta(days=1)


def _to_date(end: date, today: date) -> tuple[date, str]:
    """Clip a range ending in the future to today, and say so in the label."""
    return (today, " to date") if end >= today else (end, "")


MONTH_NAMES = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
MONTH_ABBR = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
              "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12}  # no "may": it's a word
# "may" is left out: it's an everyday word. "May 2026" still works (see _named_month).
MONTH_WORDS = {k: v for k, v in (MONTH_NAMES | MONTH_ABBR).items() if k != "may"}
NUMBER_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
                "eight": 8, "nine": 9, "ten": 10, "twelve": 12}

Handler = Callable[[re.Match, date], Period]
_RULES: list[tuple[re.Pattern, Handler]] = []


def _rule(pattern: str) -> Callable[[Handler], Handler]:
    def register(fn: Handler) -> Handler:
        _RULES.append((re.compile(pattern, re.IGNORECASE), fn))
        return fn
    return register


@_rule(r"\btoday\b")
def _today(m, today):
    return Period(today, today, "Today")


@_rule(r"\byesterday\b")
def _yesterday(m, today):
    d = today - timedelta(days=1)
    return Period(d, d, "Yesterday")


@_rule(r"\b(?:last|past|previous)\s+(\d+|" + "|".join(NUMBER_WORDS) + r")\s+(day|week|month)s?\b")
def _last_n(m, today):
    n = int(m[1]) if m[1].isdigit() else NUMBER_WORDS[m[1].lower()]
    unit = m[2].lower()
    if unit == "day":
        start = today - timedelta(days=n - 1)
    elif unit == "week":
        start = today - timedelta(weeks=n) + timedelta(days=1)
    else:
        # Same day n months back (clamped to that month's length), plus one day.
        first = add_months(today.replace(day=1), -n)
        days_in_month = calendar.monthrange(first.year, first.month)[1]
        start = first.replace(day=min(today.day, days_in_month)) + timedelta(days=1)
    return Period(start, today, f"Last {n} {unit}{'s' if n != 1 else ''}")


@_rule(r"\bthis week\b|\bweek to date\b")
def _this_week(m, today):
    return Period(today - timedelta(days=today.weekday()), today, "This week")


@_rule(r"\b(?:last|previous|past) week\b")
def _last_week(m, today):
    monday = today - timedelta(days=today.weekday() + 7)
    return Period(monday, monday + timedelta(days=6), "Last week")


@_rule(r"\bthis month\b|\bmonth to date\b|\bmtd\b")
def _this_month(m, today):
    return Period(today.replace(day=1), today, f"{today:%B %Y} to date")


@_rule(r"\b(?:last|previous|past) month\b")
def _last_month(m, today):
    first = add_months(today.replace(day=1), -1)
    return Period(*_month(first.year, first.month), f"{first:%B %Y}")


@_rule(r"\bthis quarter\b|\bquarter to date\b|\bqtd\b")
def _this_quarter(m, today):
    q = (today.month - 1) // 3 + 1
    start, _ = _quarter(today.year, q)
    return Period(start, today, f"Q{q} {today.year} to date")


@_rule(r"\b(?:last|previous|past) quarter\b")
def _last_quarter(m, today):
    q, year = (today.month - 1) // 3, today.year
    if q == 0:
        q, year = 4, year - 1
    return Period(*_quarter(year, q), f"Q{q} {year}")


@_rule(r"\bq([1-4])(?:\s+(\d{4}))?\b")
def _named_quarter(m, today):
    q = int(m[1])
    current_q = (today.month - 1) // 3 + 1
    year = int(m[2]) if m[2] else (today.year if q <= current_q else today.year - 1)
    start, end = _quarter(year, q)
    end, suffix = _to_date(end, today)
    return Period(start, end, f"Q{q} {year}{suffix}")


@_rule(r"\bthis year\b|\byear to date\b|\bytd\b")
def _this_year(m, today):
    return Period(date(today.year, 1, 1), today, f"{today.year} to date")


@_rule(r"\b(?:last|previous|past) year\b")
def _last_year(m, today):
    return Period(date(today.year - 1, 1, 1), date(today.year - 1, 12, 31), str(today.year - 1))


@_rule(r"\b(" + "|".join(sorted(MONTH_WORDS, key=len, reverse=True)) + r"|may(?=\s+\d{4}))"
       r"(?:\s+(\d{4}))?\b")
def _named_month(m, today):
    month = MONTH_WORDS.get(m[1].lower(), 5)  # the only unlisted match is "may <year>"
    year = int(m[2]) if m[2] else (today.year if month <= today.month else today.year - 1)
    start, end = _month(year, month)
    end, suffix = _to_date(end, today)
    return Period(start, end, f"{start:%B %Y}{suffix}")


def resolve_period(text: str, today: date) -> tuple[Period | None, str]:
    """The first time phrase in `text` as a Period, and `text` with that phrase removed."""
    for pattern, handler in _RULES:
        if match := pattern.search(text):
            period = handler(match, today)
            if period.start > today:
                continue  # a future month or quarter can't have sales yet
            rest = (text[: match.start()] + " " + text[match.end():]).strip()
            return period, re.sub(r"\s+", " ", rest)
    return None, text
