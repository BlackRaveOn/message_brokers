import pika


def consume_message():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.exchange_declare(exchange="messages", exchange_type="fanout")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange="messages", queue=queue_name)

    print("---Waiting for messages. To exit press CTRL+C---")

    def callback(ch, method, properties, body):
        print(body.decode("utf-8"))

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()


if __name__ == "__main__":
    consume_message()