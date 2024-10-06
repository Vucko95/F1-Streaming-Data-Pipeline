import json
import socket
from typing import Optional
import websockets
import asyncio
from packets import PacketHeader, HEADER_FIELD_TO_PACKET_TYPE

websocket_server = "ws://ws-python:3001"


async def send_data_to_websocket(race_data):
    async with websockets.connect(websocket_server, ping_interval=None) as websocket:
        full_message_str = json.dumps(race_data)
        await websocket.send(full_message_str)
        print(f"Sent data to WebSocket: {full_message_str}")


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


def _get_listener():
    try:
        print("Starting listener on localhost:20777")
        return TelemetryListener()
    except OSError as exception:
        print(f"Unable to setup connection: {exception.args[1]}")
        print("Failed to open connector, stopping.")
        exit(127)


async def main():
    listener = _get_listener()

    try:
        while True:
            packet = listener.get()
            if packet.header.packet_id == 6:
                car_telemetry_packets = packet.car_telemetry_data[19].to_dict()
                print(car_telemetry_packets)
                await send_data_to_websocket(race_data=car_telemetry_packets)
            if packet.header.packet_id == 5:
                car_setup_data = packet.car_setups[19].to_dict()
                await send_data_to_websocket(race_data=car_setup_data)
            if packet.header.packet_id == 7:
                car_status_data = packet.car_status_data[19].to_dict()
                await send_data_to_websocket(race_data=car_status_data)

    except KeyboardInterrupt:
        print("Stop")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)


