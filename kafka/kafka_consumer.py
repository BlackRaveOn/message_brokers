from confluent_kafka import Consumer, Producer
from kafka_config import KAFKA_SERVER, TOPIC, RETRY_TOPIC, ERROR_TOPIC
import argparse


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

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        decoded_msg = msg.value().decode("utf-8")

        print("Received message: {}".format(msg.value().decode("utf-8")))

        if decoded_msg == "main":
            print(f"Received main message: {decoded_msg}")
        elif decoded_msg == "retry":
            retry_producer.produce(RETRY_TOPIC, value=decoded_msg)
            retry_producer.flush()
        elif decoded_msg == "error":
            error_producer.produce(ERROR_TOPIC, value=decoded_msg)
            error_producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", help="group for consumer", required=True)
    args = parser.parse_args()
    consume_messages(group_id=args.group)
