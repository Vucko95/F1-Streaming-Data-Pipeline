# consumer_utils.py
import json
import websockets
from aiokafka import AIOKafkaConsumer
from .settings import*


async def setup_kafka_consumer():
    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',
        group_id=None,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    await consumer.start()
    return consumer


async def forward_messages_to_websockets(consumer, websocket):
    try:
        async for message in consumer:
            full_message_str = json.dumps(message.value)
            await websocket.send(full_message_str)
            print(f'Sent data: {full_message_str}')

    except websockets.ConnectionClosedError:
        print("Connection closed by the client.")