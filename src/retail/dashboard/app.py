"""Supermarket dashboard.

    streamlit run src/retail/dashboard/app.py      (or: make dashboard)

Live tab:    Postgres via the read-only role, refreshed every 5 seconds.
History tab: gold Delta tables via DuckDB, as fresh as the last `make lake`.
"""

import pandas as pd
import psycopg
import streamlit as st

from retail import config
from retail.dashboard import charts
from retail.dashboard.data import GoldLake, LiveStore, store_today

LIVE_REFRESH = "5s"

st.set_page_config(page_title="Retail Data Platform", page_icon="🛒", layout="wide")


@st.cache_resource
def live_store() -> LiveStore:
    return LiveStore(config.POSTGRES_READER_DSN)


@st.cache_resource(ttl=60)  # re-checks for gold tables built after the app started
def gold_lake() -> GoldLake:
    return GoldLake(config.LAKE_DIR)


def theme() -> charts.Theme:
    return charts.DARK if st.context.theme.type == "dark" else charts.LIGHT


def table_view(df: pd.DataFrame, **column_config) -> None:
    with st.expander("Table"):
        st.dataframe(df, hide_index=True, width="stretch", column_config=column_config)


MONEY = st.column_config.NumberColumn(format="₹%.2f")
RUPEES = st.column_config.NumberColumn(format="₹%.0f")
STATUS_LABEL = {"negative": "⛔ Negative", "out": "⛔ Out of stock", "low": "⚠️ Low"}


@st.fragment(run_every=LIVE_REFRESH)
def live_tab(store_ids: list[str]) -> None:
    live, t = live_store(), theme()
    today = store_today()
    try:
        by_store = live.sales_by_store(today)
        alerts = live.stock_alerts(store_ids)
        top = live.top_products(today, store_ids)
        prices = live.price_changes()
    except psycopg.OperationalError as exc:
        st.error(f"Can't reach Postgres ({exc.__class__.__name__}). Is `make up` running?")
        return

    selected = by_store[by_store.store_id.isin(store_ids)]
    revenue, invoices = selected.revenue.sum(), int(selected.invoices.sum())
    critical = int((alerts.status != "low").sum())

    cols = st.columns(5)
    cols[0].metric("Revenue today", charts.inr(revenue))
    cols[1].metric("Invoices", f"{invoices:,}")
    cols[2].metric("Average basket", charts.inr(revenue / invoices) if invoices else "–")
    cols[3].metric("⚠️ Low stock items", f"{len(alerts) - critical:,}")
    cols[4].metric("⛔ Out or negative", f"{critical:,}")
    st.caption(f"{today:%A %d %B %Y} (store time) · refreshes every {LIVE_REFRESH}")

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Revenue today by store")
        if invoices:
            st.altair_chart(charts.store_revenue_bars(selected, t), theme=None,
                            width="stretch")
            table_view(selected, revenue=RUPEES)
        else:
            st.info("No sales yet today. Start `make stream` and `make simulate`.")
    with right:
        st.subheader("Top products today")
        if len(top):
            st.altair_chart(charts.ranked_bars(top, "product", t, "Revenue"), theme=None,
                            width="stretch")
            table_view(top, revenue=RUPEES)
        else:
            st.info("No sales yet today.")

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Stock alerts")
        if len(alerts):
            shown = alerts.assign(status=alerts.status.map(STATUS_LABEL))
            columns = ["status", "store_id", "product", "stock_qty", "reorder_level", "updated_at"]
            st.dataframe(
                shown[columns],
                hide_index=True, width="stretch", height=320,
                column_config={"status": "Status", "store_id": "Store", "product": "Product",
                               "stock_qty": "Stock", "reorder_level": "Reorder at",
                               "updated_at": st.column_config.DatetimeColumn(
                                   "Updated", format="HH:mm:ss")},
            )
        else:
            st.success("✅ Every item is above its reorder level.")
    with right:
        st.subheader("Price changes (last 7 days)")
        if len(prices):
            arrows = prices.change_pct.map(lambda p: f"▲ {p:.1f}%" if p > 0 else f"▼ {-p:.1f}%")
            shown = prices.assign(change=arrows)
            st.dataframe(
                shown[["changed_at", "product", "old_price", "new_price", "change"]],
                hide_index=True, width="stretch",
                column_config={"changed_at": st.column_config.DatetimeColumn(
                                   "When", format="DD MMM HH:mm"),
                               "product": "Product",
                               "old_price": st.column_config.NumberColumn("Was", format="₹%.2f"),
                               "new_price": st.column_config.NumberColumn("Now", format="₹%.2f"),
                               "change": "Change"},
            )
        else:
            st.info("No price changes in the last 7 days.")


@st.cache_data(ttl=60, show_spinner=False)
def history_frames(months: int, store_ids: tuple[str, ...]) -> dict:
    gold, ids = gold_lake(), list(store_ids)
    daily = gold.daily_by_store(90, ids)
    # Today is partial (and shown live); ending the history at yesterday avoids a fake drop.
    daily = daily[daily.event_date.dt.date < store_today()]
    return {
        "through": gold.data_through(),
        "monthly": gold.monthly_by_store(months, ids),
        "daily": daily,
        "categories": gold.revenue_by_category(months, ids),
    }


def history_tab(months: int, store_ids: list[str]) -> None:
    if not gold_lake().available():
        st.info("The gold tables don't exist yet. Run `make lake`.")
        return
    t, frames = theme(), history_frames(months, tuple(store_ids))
    st.caption(f"Gold tables, built by `make lake` · data through {frames['through']:%d %b %Y}"
               " · the current month is month-to-date")

    st.subheader("Monthly revenue by store")
    st.altair_chart(charts.monthly_revenue_columns(frames["monthly"], t), theme=None,
                    width="stretch")
    table_view(frames["monthly"], revenue=RUPEES,
               month=st.column_config.DateColumn("Month", format="MMM YYYY"))

    left, right = st.columns([3, 2], gap="large")
    with left:
        st.subheader("Daily revenue, last 90 days")
        st.altair_chart(charts.daily_revenue_lines(frames["daily"], t), theme=None,
                        width="stretch")
        table_view(frames["daily"], revenue=RUPEES, avg_basket_value=MONEY,
                   event_date=st.column_config.DateColumn("Date"))
    with right:
        st.subheader(f"Revenue by category, last {months} months")
        st.altair_chart(charts.ranked_bars(frames["categories"], "category", t, "Revenue"),
                        theme=None, width="stretch")
        table_view(frames["categories"], revenue=RUPEES)


def main() -> None:
    st.title("🛒 Retail Data Platform")
    try:
        stores = live_store().stores()
    except psycopg.OperationalError:
        st.error("Can't reach Postgres. Start the stack with `make up`.")
        st.stop()

    # One filter row above everything it scopes.
    left, right, _ = st.columns([3, 1, 2])
    labels = dict(zip(stores.store_id, stores.store_id + " · " + stores.name, strict=True))
    store_ids = left.multiselect("Stores", list(labels), default=list(labels),
                                 format_func=labels.get) or list(labels)
    months = right.selectbox("History", [3, 6, 12], index=2,
                             format_func=lambda m: f"Last {m} months")

    live, history = st.tabs(["Live", "History"])
    with live:
        live_tab(store_ids)
    with history:
        history_tab(months, store_ids)


main()
