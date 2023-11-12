# main.py

import socket
import asyncio
from f1_2020_telemetry.packets import *

from f1_2020_telemetry.packets import PacketID
from kafka_utils.create_topics import create_kafka_topic
from kafka_utils.producer import initialize_kafka_producer
from telemetry_processor import (
    process_car_telemetry,
    process_car_status,
    process_lap_data,
)

topic_name = "F1Topic"
kafka_server = 'localhost:29092'
producer = initialize_kafka_producer(kafka_server)

async def process_telemetry_packet(packet):
    try:
        packet = unpack_udp_packet(packet)
        packet_id = packet.header.packetId

        if packet_id == PacketID.CAR_TELEMETRY:
            await process_car_telemetry(packet, producer, topic_name)
        elif packet_id == PacketID.CAR_STATUS:
            await process_car_status(packet, producer, topic_name)
        elif packet_id == PacketID.LAP_DATA:
            await process_lap_data(packet, producer, topic_name)
        elif packet_id == PacketID.LOBBY_INFO:
            pass

    except Exception as e:
        print(f"Error processing telemetry packet: {e}")

async def receive_and_stream_telemetry(UDP_IP, UDP_PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    while True:
        data, addr = sock.recvfrom(4096)
        await process_telemetry_packet(data)

if __name__ == "__main__":
    UDP_IP = "0.0.0.0"
    UDP_PORT = 20777

    loop = asyncio.get_event_loop()

    try:
        create_kafka_topic(topic_name)
        loop.create_task(receive_and_stream_telemetry(UDP_IP, UDP_PORT))
        loop.run_forever()

    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
        loop.close()
