import asyncio
import websockets
from kafka_utils.settings import *
from kafka_utils.consumer import forward_messages_to_websockets, setup_kafka_consumer

async def start_websocket_server(websocket, path):
    print(f"Accepted connection from {websocket.remote_address}")

    kafka_consumer = await setup_kafka_consumer()

    try:
        await forward_messages_to_websockets(kafka_consumer, websocket)
    finally:
        await kafka_consumer.stop()

if __name__ == "__main__":
    socket_host = '0.0.0.0'
    socket_port = 8777

    server = websockets.serve(start_websocket_server, socket_host, socket_port)
    print(f"WebSocket server listening on {socket_host}:{socket_port}")

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(server)
        loop.run_forever()
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())
