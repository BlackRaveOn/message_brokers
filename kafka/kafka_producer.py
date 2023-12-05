from confluent_kafka import Producer
import argparse
import json
from bson import json_util


from kafka_config import KAFKA_SERVER, TOPIC


def produce_message(message: str, message_type: str):
    producer_conf = {
        "bootstrap.servers": KAFKA_SERVER,
    }

    producer = Producer(producer_conf)

    data = {"type": message_type, "message": message}

    producer.produce(
        TOPIC, value=json.dumps(data, default=json_util.default).encode("utf-8")
    )
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--msg", help="message to produce in Kafka", required=True)
    parser.add_argument(
        "--type", help="type of message to produce in Kafka", required=True
    )
    args = parser.parse_args()
    produce_message(message=args.msg, message_type=args.type)
