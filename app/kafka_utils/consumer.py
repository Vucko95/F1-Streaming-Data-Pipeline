# consumer_utils.py
import json
from settings import *
from kafka import KafkaConsumer




def initialize_kafka_consumer(topic_name, kafka_server):
    consumer = KafkaConsumer(
        topic_name=topic_name,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliset',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer


def consume_and_print_messages(consumer):
    try:
        for message in consumer:
            full_message_str = json.dumps(message.value)
            print(f'Received data: {full_message_str}')

            # Process the message as needed

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

consumer = initialize_kafka_consumer(topic_name, kafka_server)
consume_and_print_messages(consumer)
print('test')








# async def setup_kafka_consumer():
#     consumer = AIOKafkaConsumer(
#         topic_name,
#         bootstrap_servers=kafka_server,
#         auto_offset_reset='earliest',
#         group_id=None,
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
#     await consumer.start()
#     return consumer


# async def forward_messages_to_websockets(consumer, websocket):
#     try:
#         async for message in consumer:
#             full_message_str = json.dumps(message.value)
#             await websocket.send(full_message_str)
#             print(f'Sent data: {full_message_str}')

#     except websockets.ConnectionClosedError:
#         print("Connection closed by the client.")