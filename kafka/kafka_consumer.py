from confluent_kafka import Consumer, Producer
from kafka_config import KAFKA_SERVER, TOPIC, RETRY_TOPIC, ERROR_TOPIC
import argparse
import json
from bson import json_util


def consume_messages(group_id: str):

    consumer_conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_conf)
    topics = [TOPIC]

    consumer.subscribe(topics)

    producer_conf = {
        "bootstrap.servers": KAFKA_SERVER,
    }

    retry_producer = Producer(producer_conf)
    error_producer = Producer(producer_conf)

    print("---Waiting for messages. To exit press CTRL+C---")

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        decoded_msg = json.loads(msg.value().decode("utf-8"))

        print(f"Received message: {decoded_msg}")

        if decoded_msg["type"] == "main":
            print(f"Received main message: {decoded_msg}")
        elif decoded_msg["type"] == "retry":
            retry_producer.produce(
                RETRY_TOPIC,
                value=json.dumps(decoded_msg, default=json_util.default).encode(
                    "utf-8"
                ),
            )
            retry_producer.flush()
            print(f"Received retry message: {decoded_msg}. Send it to retry topic")
        elif decoded_msg["type"] == "error":
            error_producer.produce(
                ERROR_TOPIC,
                value=json.dumps(decoded_msg, default=json_util.default).encode(
                    "utf-8"
                ),
            )
            error_producer.flush()
            print(f"Received retry message: {decoded_msg}. Send it to error topic")
        else:
            print(f"Unknown message type!!! Message: {decoded_msg} was ignored")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", help="group for consumer", required=True)
    args = parser.parse_args()
    consume_messages(group_id=args.group)
