import json
import websockets
from kafka_utils.settings import *
from kafka import KafkaConsumer
import asyncio
from time import sleep

async def initialize_websocket():
    uri = "ws://192.168.0.160:3001"
    return await websockets.connect(uri)

def initialize_kafka_consumer(topic_name, kafka_server):
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe([topic_name])
    return consumer

async def consume_and_send_to_websocket(consumer, websocket):
    try:
        for message in consumer:
            full_message_str = json.dumps(message.value)
            asyncio.create_task(websocket.send(full_message_str))
            print('------')
            print(f'Sent data to WebSocket: {full_message_str}')
            print('------')
            # sleep(1)
            await asyncio.sleep(0.01)

    except Exception as e:
        print(f"Error: {e}")
    # finally:
        # consumer.close()

async def main():
    consumer = initialize_kafka_consumer(topic_name, kafka_server)
    websocket = await initialize_websocket()

    try:
        await consume_and_send_to_websocket(consumer, websocket)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(main())
