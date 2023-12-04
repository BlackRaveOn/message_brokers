from confluent_kafka import Producer


def produce_message():
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
    }

    producer = Producer(producer_conf)
    topic = 'your_topic_name'
    message = 'Hello, Kafka!'

    producer.produce(topic, message)
    producer.flush()

if __name__ == '__main__':
    produce_message()
