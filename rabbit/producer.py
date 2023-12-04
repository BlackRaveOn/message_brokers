import pika
import argparse


def produce_message(message: str = "Hello world!"):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()

    channel.exchange_declare(exchange="messages", exchange_type="fanout")

    channel.basic_publish(exchange="messages", routing_key="", body=message)
    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--msg", help='string to produce in RabbitMQ', required=False
    )
    args = parser.parse_args()
    produce_message(message = args.msg)