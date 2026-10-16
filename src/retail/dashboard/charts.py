"""Altair charts for the dashboard, following one set of rules:

* Each store keeps one colour everywhere (categorical slots 1-3, validated for
  colour-blind separation in light and dark mode), mapped by store_id, so a
  filter never repaints the stores that remain.
* Charts not about stores (products, categories) use one neutral series colour
  that no store uses.
* Thin marks with 4px rounded ends, 2px lines, hairline grid; text is ink, never
  the series colour; tooltips everywhere and a table view beside every chart.
"""

from dataclasses import dataclass

import altair as alt
import pandas as pd

STORE_IDS = ["BLR01", "HYD01", "PUN01"]  # fixed order = fixed colour


@dataclass(frozen=True)
class Theme:
    stores: tuple[str, str, str]
    single: str  # one-series charts that aren't about a store
    surface: str
    ink: str
    ink_secondary: str
    muted: str
    grid: str
    baseline: str


LIGHT = Theme(("#2a78d6", "#eb6834", "#1baf7a"), "#6250d6", "#fcfcfb",
              "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7")
DARK = Theme(("#3987e5", "#d95926", "#199e70"), "#9085e9", "#1a1a19",
             "#f0efec", "#c3c2b7", "#898781", "#2c2c2a", "#383835")


def inr(value: float) -> str:
    """Indian digit grouping: 22003208 -> ₹2,20,03,208."""
    whole = f"{abs(value):.0f}"
    head, tail = whole[:-3], whole[-3:]
    groups = []
    while len(head) > 2:
        head, groups = head[:-2], [head[-2:], *groups]
    grouped = ",".join(([head] if head else []) + groups + [tail])
    return f"{'-' if value < 0 else ''}₹{grouped}"


def _lakh_axis(title: str) -> alt.Axis:
    return alt.Axis(
        title=title, tickCount=5,
        labelExpr="datum.value == 0 ? '₹0' : '₹' + format(datum.value / 100000, ',.1~f') + 'L'",
    )


def _store_color(theme: Theme, legend: bool = True) -> alt.Color:
    return alt.Color(
        "store_id:N", title="Store",
        scale=alt.Scale(domain=STORE_IDS, range=list(theme.stores)),
        legend=alt.Legend(orient="top", direction="horizontal") if legend else None,
    )


def _style(chart: alt.Chart, theme: Theme) -> alt.Chart:
    return (
        chart.configure(background="transparent", font="system-ui, -apple-system, sans-serif")
        .configure_view(stroke=None)
        .configure_axis(gridColor=theme.grid, gridWidth=1, domainColor=theme.baseline,
                        tickColor=theme.baseline, labelColor=theme.muted,
                        titleColor=theme.ink_secondary, labelFontSize=11, titleFontSize=11,
                        titleFontWeight="normal")
        .configure_legend(labelColor=theme.ink_secondary, titleColor=theme.ink_secondary,
                          symbolType="circle")
    )


def store_revenue_bars(df: pd.DataFrame, theme: Theme) -> alt.Chart:
    """Today's revenue per store: horizontal bars, value labelled at the bar end."""
    df = df.assign(label=df.store_id + " · " + df.name, value=df.revenue.map(inr))
    base = alt.Chart(df).encode(
        y=alt.Y("label:N", title=None, sort=STORE_IDS, axis=alt.Axis(labelLimit=200)),
        # Headroom past the longest bar so its end label stays inside the plot.
        x=alt.X("revenue:Q", axis=_lakh_axis("Revenue today"),
                scale=alt.Scale(domain=[0, max(df.revenue.max(), 1) * 1.25], nice=False)),
    )
    bars = base.mark_bar(cornerRadiusEnd=4, size=18).encode(
        color=_store_color(theme, legend=False),
        tooltip=[alt.Tooltip("label:N", title="Store"), alt.Tooltip("value:N", title="Revenue"),
                 alt.Tooltip("invoices:Q", title="Invoices", format=","),
                 alt.Tooltip("units:Q", title="Units", format=",")],
    )
    labels = base.mark_text(align="left", dx=6, color=theme.ink_secondary).encode(text="value:N")
    return _style((bars + labels).properties(height=130), theme)


def ranked_bars(df: pd.DataFrame, label: str, theme: Theme, title: str) -> alt.Chart:
    """Single-series horizontal bars sorted by revenue (products, categories)."""
    df = df.assign(value=df.revenue.map(inr))
    chart = alt.Chart(df).mark_bar(cornerRadiusEnd=4, size=14, color=theme.single).encode(
        y=alt.Y(f"{label}:N", title=None, sort="-x", axis=alt.Axis(labelLimit=260)),
        x=alt.X("revenue:Q", title=title, axis=alt.Axis(
            tickCount=5,
            labelExpr="datum.value == 0 ? '₹0' : datum.value >= 100000"
                      " ? '₹' + format(datum.value / 100000, ',.1~f') + 'L'"
                      " : '₹' + format(datum.value / 1000, ',.1~f') + 'k'",
        )),
        tooltip=[alt.Tooltip(f"{label}:N", title=label.title()),
                 alt.Tooltip("value:N", title="Revenue"),
                 alt.Tooltip("units:Q", title="Units", format=",")],
    )
    return _style(chart.properties(height=max(120, 24 * len(df))), theme)


def monthly_revenue_columns(df: pd.DataFrame, theme: Theme) -> alt.Chart:
    """Revenue per month, stacked by store (part-to-whole over time)."""
    df = df.assign(value=df.revenue.map(inr))
    chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4,
                                   stroke=theme.surface, strokeWidth=2).encode(
        x=alt.X("yearmonth(month):O", title=None, axis=alt.Axis(format="%b %y", labelAngle=0)),
        y=alt.Y("revenue:Q", stack=True, axis=_lakh_axis("Revenue")),
        color=_store_color(theme),
        order=alt.Order("store_id:N"),
        tooltip=[alt.Tooltip("yearmonth(month):O", title="Month", format="%B %Y"),
                 alt.Tooltip("store_id:N", title="Store"), alt.Tooltip("value:N", title="Revenue")],
    )
    return _style(chart.properties(height=280), theme)


def daily_revenue_lines(df: pd.DataFrame, theme: Theme) -> alt.Chart:
    """Daily revenue per store, with a crosshair tooltip showing every store at once."""
    stores = [s for s in STORE_IDS if s in set(df.store_id)]
    x = alt.X("event_date:T", title=None, axis=alt.Axis(format="%d %b", tickCount=8))
    base = alt.Chart(df).encode(x=x, y=alt.Y("revenue:Q", axis=_lakh_axis("Revenue")),
                                color=_store_color(theme))
    lines = base.mark_line(strokeWidth=2)

    nearest = alt.selection_point(nearest=True, on="pointerover", fields=["event_date"],
                                  empty=False)
    crosshair = (
        alt.Chart(df).transform_pivot("store_id", value="revenue", groupby=["event_date"])
        .mark_rule(color=theme.baseline, strokeWidth=1)
        .encode(
            x=x,
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip("event_date:T", title="Date", format="%a %d %b %Y")]
            + [alt.Tooltip(f"{s}:Q", title=s, format=",.0f") for s in stores],
        )
        .add_params(nearest)
    )
    points = base.mark_point(filled=True, size=64, stroke=theme.surface, strokeWidth=2).encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )
    # Direct labels at the right end of each line (ink-coloured text, store-coloured dot).
    last = df[df.event_date == df.event_date.max()]
    end_dots = alt.Chart(last).mark_point(filled=True, size=40).encode(
        x=x, y="revenue:Q", color=_store_color(theme))
    end_labels = alt.Chart(last).mark_text(align="left", dx=8, color=theme.ink_secondary).encode(
        x=x, y="revenue:Q", text="store_id:N")
    chart = alt.layer(lines, crosshair, points, end_dots, end_labels).properties(height=280)
    return _style(chart, theme)
