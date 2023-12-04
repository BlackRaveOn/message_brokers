from confluent_kafka import Consumer, KafkaError

def consume_messages(consumer, topics):
    consumer.subscribe(topics)

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print('Received message: {}'.format(msg.value().decode('utf-8')))

    consumer.close()

def main():
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'your_consumer_group_id',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    topics = ['your_topic_name']

    consume_messages(consumer, topics)

if __name__ == '__main__':
    main()
