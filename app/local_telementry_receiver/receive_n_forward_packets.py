import json, socket, websockets, asyncio
from typing import Optional
from packets import PacketHeader, HEADER_FIELD_TO_PACKET_TYPE

websocket_url = "ws://localhost:3001"
nifi_udp_port = 20888
nifi_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
CAR_INDEX = 19


async def transmit_to_websocket(race_data: dict) -> None:
    try:
        async with websockets.connect(websocket_url, ping_interval=None) as websocket:
            serialized_race_data = json.dumps(race_data)
            await websocket.send(serialized_race_data)
            print(f"Sent data to WebSocket: {serialized_race_data}")
            transmit_to_nifi(serialized_race_data)
    except websockets.exceptions.ConnectionClosedError:
        print("WebSocket connection closed unexpectedly")
    except Exception as e:
        print(f"Failed to send data to WebSocket: {e}")


def transmit_to_nifi(data):
    nifi_socket.sendto(data.encode(), ("localhost", nifi_udp_port))
    print(f"Sent data to NiFi: {data}")


class TelemetryReceiver:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        if not port:
            port = 20777

        if not host:
            host = "0.0.0.0"

        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.socket.bind((host, port))

    def receive_packet(self):
        received_packet = self.socket.recv(2048)
        header = PacketHeader.from_buffer_copy(received_packet)
        key = (header.packet_format, header.packet_version, header.packet_id)
        return HEADER_FIELD_TO_PACKET_TYPE[key].unpack(received_packet)


def _initialize_listener():
    try:
        print("Starting listener on localhost:20777")
        return TelemetryReceiver()
    except OSError as exception:
        print(f"Unable to setup connection: {exception.args[1]}")
        print("Failed to open connector, stopping.")
        exit(127)


async def main():
    listener = _initialize_listener()

    try:
        while True:
            packet = listener.receive_packet()
            if packet.header.packet_id == 6:
                car_telemetry_packets = packet.car_telemetry_data[CAR_INDEX].to_dict()
                await transmit_to_websocket(race_data=car_telemetry_packets)
            elif packet.header.packet_id == 7:
                car_status_packets = packet.car_status_data[CAR_INDEX].to_dict()
                await transmit_to_websocket(race_data=car_status_packets)

    except KeyboardInterrupt:
        print("Stopping")
    finally:
        nifi_socket.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)
