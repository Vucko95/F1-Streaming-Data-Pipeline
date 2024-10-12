import json, socket, websockets, asyncio, os

websocket_url = "ws://localhost:3001"
nifi_udp_port = 20888
nifi_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
folder_name = "simulation_data"


json_file = os.path.join(folder_name, "car_telemetry_data.json")


def read_json_file(filepath):
    with open(filepath, "r") as file:
        data = json.load(file)
    return data


def transmit_to_nifi(data):
    nifi_socket.sendto(data.encode(), ("localhost", nifi_udp_port))
    # print(f"Sent data to NiFi: {data}")


async def transmit_to_websocket_and_nifi(race_data: dict) -> None:
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


async def main():
    try:
        with open(json_file, "r") as file:
            packets = json.load(file)

            for packet in packets:
                await transmit_to_websocket_and_nifi(packet)

                await asyncio.sleep(0.02)

    except KeyboardInterrupt:
        print("Stopping transmission...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        nifi_socket.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Failed to run the main function: {e}")
