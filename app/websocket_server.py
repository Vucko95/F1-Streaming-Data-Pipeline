import asyncio
import websockets

connected_clients = set()

async def handle_websocket(websocket, path):
    try:
        print(f"Connection established from {websocket.remote_address}")

        connected_clients.add(websocket)

        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5)  # Adjust timeout as needed
                # print(f"Received message: {message}")

                for client in connected_clients:
                    await client.send(message)
                    print(f"Sent message to {client.remote_address}: {message}")

            except asyncio.TimeoutError:
                # print('SERVER PINGING')
                await websocket.ping()  
                # print(f'Sent ping to {websocket.remote_address}')

    except websockets.exceptions.ConnectionClosedError:
        print(f"Connection closed by the client.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        connected_clients.remove(websocket)

if __name__ == "__main__":
    server_host = "192.168.0.160"
    server_port = 3001

    start_server = websockets.serve(
        handle_websocket,
        server_host,
        server_port,
        ping_interval=None
    )

    print(f"WebSocket server listening on {server_host}:{server_port}")

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
