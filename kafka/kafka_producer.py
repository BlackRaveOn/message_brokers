from confluent_kafka import Producer
import argparse

from kafka_config import KAFKA_SERVER, TOPIC


def produce_message(message: str):
    producer_conf = {
        "bootstrap.servers": KAFKA_SERVER,
    }

    producer = Producer(producer_conf)

    producer.produce(TOPIC, message)
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--msg", help="string to produce in Kafka", required=False)
    args = parser.parse_args()
    produce_message(message=args.msg)
