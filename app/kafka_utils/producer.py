import random, string
from kafka import KafkaProducer
import json, time


def initialize_kafka_producer(kafka_server):
    return KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def publish_messages_to_kafka_socket(producer, topic_name, messages):
    try:
        random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        producer.send(topic=topic_name, key=random_key, value=messages)
        # print(f"Message {messages} ADDED TO TOPIC {topic_name}")
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")






def publish_messages_to_kafka(producer,topic_name,messages):
    try:
        for message in messages:
            random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            producer.send(topic=topic_name, key=random_key, value=message)
            print(f"Message {message} ADDED TO TOPIC {topic_name}")
            # time.sleep(3)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
def publish_single_message_to_kafka(producer,topic_name,message):
    try:
        random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        producer.send(topic=topic_name, key=random_key, value=message)
        print(f"Message {message} ADDED TO TOPIC {topic_name}")
        # time.sleep(3)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")

def read_json_file(file_name):
    try:
        with open(file_name) as json_file:
            json_array = json.load(json_file)
            return json_array
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return []
    

