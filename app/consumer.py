import asyncio
import websockets
import json
from aiokafka import AIOKafkaConsumer
from kafka_utils.settings import * 

async def forward_messages_to_websockets(consumer, websocket):
    try:
        async for message in consumer:
            full_message_str = json.dumps(message.value)
            await websocket.send(full_message_str)
            print(f'Sent data: {full_message_str}')

    except websockets.ConnectionClosedError:
        print("Connection closed by the client.")

async def serve_websocket(websocket, path):
    print(f"Accepted connection from {websocket.remote_address}")


    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',
        group_id=None,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    await consumer.start()

    await forward_messages_to_websockets(consumer, websocket)

    await consumer.stop()

if __name__ == "__main__":
    socket_host = '0.0.0.0'
    socket_port = 8777

    server = websockets.serve(
        serve_websocket, socket_host, socket_port
    )

    print(f"WebSocket server listening on {socket_host}:{socket_port}")

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(server)
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())
