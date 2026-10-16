"""Pipeline health signals that need no running service besides Kafka and Postgres.

* stream_lag      messages per topic the streaming job has not processed yet,
                  from its checkpoint (committed offsets) vs Kafka's high watermarks
* last_batch_at   when the streaming job last committed a micro-batch
* gold_built_at   when the gold tables were last built (see qa.engine)
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from retail import config


@dataclass(frozen=True)
class TopicLag:
    topic: str
    committed: int  # next offset the job will read, summed over partitions
    latest: int  # high watermark, summed over partitions
    lag: int


def _latest_commit(checkpoint: Path) -> int | None:
    """Id of the newest batch the query committed (Spark writes commits/<batch_id>)."""
    commits = [int(p.name) for p in (checkpoint / "commits").glob("[0-9]*") if p.is_file()]
    return max(commits) if commits else None


def committed_offsets(checkpoint: Path) -> dict[str, dict[int, int]] | None:
    """Kafka offsets covered by the last committed batch: {topic: {partition: offset}}.

    offsets/<id> holds a version line, a metadata line, then one JSON line per
    source; this job has a single Kafka source.
    """
    batch = _latest_commit(checkpoint)
    if batch is None:
        return None
    lines = (checkpoint / "offsets" / str(batch)).read_text().splitlines()
    source = json.loads(lines[2])
    return {topic: {int(p): o for p, o in parts.items()} for topic, parts in source.items()}


def last_batch_at(checkpoint: Path) -> datetime | None:
    batch = _latest_commit(checkpoint)
    if batch is None:
        return None
    mtime = (checkpoint / "commits" / str(batch)).stat().st_mtime
    return datetime.fromtimestamp(mtime, UTC)


def stream_lag(checkpoint: Path, bootstrap: str = config.KAFKA_BOOTSTRAP) -> list[TopicLag]:
    committed = committed_offsets(checkpoint) or {}
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "retail-monitor",
                         "enable.auto.commit": False})
    try:
        result = []
        topics = [config.TOPIC_INVOICES, config.TOPIC_PRICE_UPDATES, config.TOPIC_STOCK_RECEIPTS]
        for topic in topics:
            partitions = consumer.list_topics(topic, timeout=5).topics[topic].partitions
            latest = done = 0
            for p in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
                latest += high
                # A partition the job hasn't read yet counts from its earliest offset.
                done += committed.get(topic, {}).get(p, low)
            result.append(TopicLag(topic, done, latest, latest - done))
        return result
    finally:
        consumer.close()
