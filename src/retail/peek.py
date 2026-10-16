"""Print the most recent messages on a Kafka topic, newest last.

    python -m retail.peek invoices -n 3
"""

import argparse
import sys

from confluent_kafka import Consumer, KafkaException, TopicPartition

from retail import config


def latest(topic: str, n: int, bootstrap: str = config.KAFKA_BOOTSTRAP) -> list:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "retail-peek",
        "enable.auto.commit": False,  # read-only: never move a consumer group's offsets
    })
    try:
        metadata = consumer.list_topics(topic, timeout=5).topics[topic]
        if metadata.error is not None:
            raise KafkaException(metadata.error)

        # The newest n messages overall are among the newest n of each partition.
        assignment, remaining = [], 0
        for partition in metadata.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), 5)
            start = max(low, high - n)
            if start < high:
                assignment.append(TopicPartition(topic, partition, start))
                remaining += high - start
        consumer.assign(assignment)

        messages = []
        while remaining > 0:
            msg = consumer.poll(5)
            if msg is None:
                break  # partition went quiet; show what we have
            if msg.error() is None:
                messages.append(msg)
            remaining -= 1
        messages.sort(key=lambda m: m.timestamp()[1])
        return messages[-n:]
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("topic")
    parser.add_argument("-n", type=int, default=3, help="number of messages")
    args = parser.parse_args()
    try:
        messages = latest(args.topic, args.n)
    except KafkaException as exc:
        sys.exit(f"cannot read {args.topic!r} from {config.KAFKA_BOOTSTRAP}: {exc}")
    if not messages:
        print(f"{args.topic} is empty")
    for m in messages:
        key = m.key().decode() if m.key() else ""
        print(f"[p{m.partition()} @{m.offset()}] {key}\t{m.value().decode()}")


if __name__ == "__main__":
    main()
