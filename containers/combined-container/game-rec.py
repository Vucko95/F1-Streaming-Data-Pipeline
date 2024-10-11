import json
import socket
import websockets
import asyncio
from typing import Optional
from packets import PacketHeader, HEADER_FIELD_TO_PACKET_TYPE

connected_clients = set()
websocket_port = 3001

# Handle WebSocket connections
async def handle_websocket(websocket, path):
    try:
        print(f"Connection established from {websocket.remote_address}")
        connected_clients.add(websocket)

        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5)  # Adjust timeout as needed
                for client in connected_clients:
                    await client.send(message)
                    print(f"Sent message to {client.remote_address}: {message}")

            except asyncio.TimeoutError:
                await websocket.ping()  # Keep the connection alive with pings
    except websockets.exceptions.ConnectionClosedError:
        print(f"Connection closed by the client.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        connected_clients.remove(websocket)


# Send telemetry data to all connected WebSocket clients
async def send_data_to_websocket(race_data):
    if connected_clients:
        full_message_str = json.dumps(race_data)
        for client in connected_clients:
            await client.send(full_message_str)
        print(f"Sent data to WebSocket clients: {full_message_str}")


# Telemetry listener for F1 game data
class TelemetryListener:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        if not port:
            port = 20777
        if not host:
            host = "0.0.0.0"

        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.socket.bind((host, port))

    def get(self):
        packet = self.socket.recv(2048)
        header = PacketHeader.from_buffer_copy(packet)
        key = (header.packet_format, header.packet_version, header.packet_id)
        return HEADER_FIELD_TO_PACKET_TYPE[key].unpack(packet)


# Start listening for telemetry data
def _get_listener():
    try:
        print("Starting listener on localhost:20777")
        return TelemetryListener()
    except OSError as exception:
        print(f"Unable to setup connection: {exception.args[1]}")
        exit(127)


async def telemetry_receiver():
    listener = _get_listener()

    try:
        while True:
            packet = listener.get()
            if packet.header.packet_id == 6:
                car_telemetry_packets = packet.car_telemetry_data[19].to_dict()
                await send_data_to_websocket(race_data=car_telemetry_packets)
            if packet.header.packet_id == 7:
                car_status_data = packet.car_status_data[19].to_dict()
                await send_data_to_websocket(race_data=car_status_data)

    except KeyboardInterrupt:
        print("Stop receiving telemetry data.")


# Main function that runs both the telemetry receiver and WebSocket server
async def main():
    # Start WebSocket server
    start_server = websockets.serve(handle_websocket, "0.0.0.0", websocket_port, ping_interval=None)
    print(f"WebSocket server listening on 0.0.0.0:{websocket_port}")

    # Run WebSocket server and telemetry receiver together
    await asyncio
