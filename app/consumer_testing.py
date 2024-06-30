import json
import websockets
from kafka_utils.settings import *
from kafka import KafkaConsumer
import asyncio
import time


async def initialize_websocket():
    # return await websockets.connect(websocket_server,ping_timeout=20)
    return await websockets.connect(websocket_server, ping_interval=None)


def initialize_kafka_consumer(topic_name, kafka_server):
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_server,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    consumer.subscribe([topic_name])
    return consumer


async def consume_and_send_to_websocket(consumer, websocket):
    try:
        for message in consumer:
            full_message_str = json.dumps(message.value)
            # time.sleep(0.5)
            # await websocket.send(full_message_str)
            # await asyncio.wait_for( websocket.send(full_message_str),timeout=1)
            await asyncio.wait_for(websocket.send(full_message_str), timeout=5)
            print(f"Sent data to WebSocket: {full_message_str}")

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error: {e}")


# $.raceData['speed']
async def main():
    consumer = initialize_kafka_consumer(topic_name, kafka_server)
    websocket = await initialize_websocket()
    try:
        await consume_and_send_to_websocket(consumer, websocket)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Unhandled exception: {e}")
