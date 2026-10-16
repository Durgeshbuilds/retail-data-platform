"""Live simulator: streams invoices to Kafka, with price changes and restock deliveries.

Three event streams come out of one loop:

* invoices        arrive as a Poisson process at --rate per second, spread across
                  stores by size. Prices come from Postgres at start-up and are kept
                  in step with the price changes this process emits.
* price-updates   one random product every --price-every seconds, up or down a few %.
* stock-receipts  every --restock-every seconds the simulator looks for stock at or
                  below its reorder level (as a store's replenishment system would)
                  and schedules a delivery --lead-time seconds later.

Stock is only read here, never written: applying sales and receipts to Postgres is
the streaming job's responsibility (phase 3).

    python -m retail.simulator.live --rate 5
"""

import argparse
import logging
import random
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol

import psycopg
from confluent_kafka import KafkaException, Producer
from pydantic import BaseModel

from retail import config
from retail.catalog import TARGET_STOCK_MULTIPLE, load_products, load_stores
from retail.events import PriceUpdate, StockReceipt, utc_now
from retail.simulator.invoices import InvoiceFactory

log = logging.getLogger(__name__)

# Don't reorder the same item again until this long after its delivery, giving the
# streaming job time to apply the receipt to inventory_current.
REORDER_COOLDOWN_S = 60.0
REPORT_EVERY_S = 10.0


class Sink(Protocol):
    failed: int

    def send(self, topic: str, key: str, event: BaseModel) -> None: ...
    def close(self) -> None: ...


class KafkaSink:
    def __init__(self, bootstrap: str) -> None:
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,  # broker drops duplicates caused by producer retries
            "linger.ms": 20,
            "compression.type": "lz4",
            "partitioner": "murmur2_random",  # same key hashing as Java clients (Spark)
        })
        self._producer.list_topics(timeout=5)  # fail fast if Kafka is not reachable
        self.failed = 0

    def send(self, topic: str, key: str, event: BaseModel) -> None:
        payload = event.model_dump_json()
        while True:
            try:
                self._producer.produce(topic, key=key, value=payload,
                                       on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)  # local queue is full; wait for deliveries
        self._producer.poll(0)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery to %s failed: %s", msg.topic(), err)

    def close(self) -> None:
        if remaining := self._producer.flush(10):
            log.warning("%d messages were not delivered", remaining)


class StdoutSink:
    """Prints events instead of sending them; handy for eyeballing the payloads."""

    failed = 0

    def send(self, topic: str, key: str, event: BaseModel) -> None:
        print(f"{topic}\t{key}\t{event.model_dump_json()}", flush=True)

    def close(self) -> None:
        pass


@dataclass(frozen=True)
class PendingDelivery:
    due: float  # time.monotonic() value
    store_id: str
    product_id: str
    qty: int


class LiveSimulator:
    def __init__(self, sink: Sink, conn: psycopg.Connection, rng: random.Random,
                 rate: float, price_every: float, restock_every: float,
                 lead_time: float) -> None:
        self.sink, self.conn, self.rng = sink, conn, rng
        self.rate, self.price_every = rate, price_every
        self.restock_every, self.lead_time = restock_every, lead_time

        self.stores = load_stores()
        self.products = load_products()
        self.factory = InvoiceFactory(self.products, rng)
        self.prices = dict(conn.execute("SELECT product_id, current_price FROM products"))
        if not self.prices:
            sys.exit("products table is empty: run `make seed` first")

        self.pending: list[PendingDelivery] = []
        self.on_order_until: dict[tuple[str, str], float] = {}
        self.sent: Counter[str] = Counter()

    def run(self, duration: float | None) -> None:
        now = time.monotonic()
        deadline = now + duration if duration else None
        next_invoice, next_price = now, now + self.price_every
        next_restock, next_report = now, now + REPORT_EVERY_S

        while deadline is None or now < deadline:
            if now >= next_invoice:
                self._send_invoice()
                # Exponential gaps give Poisson arrivals. Never fall more than 1s
                # behind, or a pause would be followed by a burst.
                next_invoice = max(next_invoice + self.rng.expovariate(self.rate), now - 1.0)
            if now >= next_price:
                self._change_price()
                next_price += self.price_every
            if now >= next_restock:
                self._check_reorders(now)
                next_restock += self.restock_every
            self._deliver_due(now)
            if now >= next_report:
                log.info("sent so far: %s", dict(self.sent))
                next_report += REPORT_EVERY_S

            wake = min(next_invoice, next_price, next_restock, next_report,
                       *(d.due for d in self.pending))
            if deadline is not None:
                wake = min(wake, deadline)
            time.sleep(max(0.0, wake - time.monotonic()))
            now = time.monotonic()

    def _send_invoice(self) -> None:
        store = self.rng.choices(self.stores, [s.size_factor for s in self.stores])[0]
        invoice = self.factory.build(store.store_id, utc_now(), self.prices)
        # Keyed by invoice_id, not store_id: three store keys would pile onto one or two
        # partitions, and invoices need no per-store ordering (stock changes add up).
        self.sink.send(config.TOPIC_INVOICES, invoice.invoice_id, invoice)
        self.sent["invoices"] += 1

    def _change_price(self) -> None:
        product = self.rng.choice(self.products)
        old = self.prices[product.product_id]
        factor = Decimal(str(round(self.rng.uniform(0.90, 1.12), 3)))
        new = max((old * factor).quantize(Decimal("1"), ROUND_HALF_UP), Decimal("1"))
        new = new.quantize(Decimal("0.01"))
        if new == old:
            new += 1
        update = PriceUpdate(product_id=product.product_id, old_price=old, new_price=new,
                             effective_at=utc_now())
        self.sink.send(config.TOPIC_PRICE_UPDATES, product.product_id, update)
        self.prices[product.product_id] = new
        self.sent["price-updates"] += 1

    def _check_reorders(self, now: float) -> None:
        rows = self.conn.execute(
            "SELECT store_id, product_id, stock_qty, reorder_level"
            " FROM inventory_current WHERE stock_qty <= reorder_level"
        ).fetchall()
        for store_id, product_id, stock_qty, reorder_level in rows:
            key = (store_id, product_id)
            if self.on_order_until.get(key, 0.0) > now:
                continue
            qty = reorder_level * TARGET_STOCK_MULTIPLE - stock_qty
            due = now + self.lead_time
            self.pending.append(PendingDelivery(due, store_id, product_id, qty))
            self.on_order_until[key] = due + REORDER_COOLDOWN_S

    def _deliver_due(self, now: float) -> None:
        due = [d for d in self.pending if d.due <= now]
        if not due:
            return
        self.pending = [d for d in self.pending if d.due > now]
        for d in due:
            receipt = StockReceipt(
                receipt_id=str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
                store_id=d.store_id, product_id=d.product_id, qty=d.qty,
                received_at=utc_now(),
            )
            self.sink.send(config.TOPIC_STOCK_RECEIPTS, d.store_id, receipt)
            self.sent["stock-receipts"] += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream simulated store events to Kafka.")
    parser.add_argument("--rate", type=float, default=5.0, help="invoices per second")
    parser.add_argument("--price-every", type=float, default=30.0,
                        help="seconds between price changes")
    parser.add_argument("--restock-every", type=float, default=10.0,
                        help="seconds between reorder checks")
    parser.add_argument("--lead-time", type=float, default=20.0,
                        help="seconds from reorder to delivery")
    parser.add_argument("--duration", type=float, help="stop after this many seconds")
    parser.add_argument("--seed", type=int, help="random seed, for reproducible runs")
    parser.add_argument("--stdout", action="store_true",
                        help="print events instead of sending to Kafka")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stderr)

    try:
        sink: Sink = StdoutSink() if args.stdout else KafkaSink(config.KAFKA_BOOTSTRAP)
    except KafkaException as exc:
        sys.exit(f"cannot reach Kafka at {config.KAFKA_BOOTSTRAP}: {exc}")

    with psycopg.connect(config.POSTGRES_DSN, autocommit=True) as conn:
        sim = LiveSimulator(sink, conn, random.Random(args.seed), args.rate,
                            args.price_every, args.restock_every, args.lead_time)
        log.info("streaming ~%.1f invoices/s; Ctrl-C to stop", args.rate)
        try:
            sim.run(args.duration)
        except KeyboardInterrupt:
            pass
        finally:
            sink.close()
            log.info("stopped. sent %s, failed deliveries: %d", dict(sim.sent), sink.failed)


if __name__ == "__main__":
    main()
