import asyncio, socket
from f1_2020_telemetry.packets import PacketID, unpack_udp_packet
from kafka_utils.create_topic import create_topic
from kafka_utils.producer import initialize_kafka_producer
from kafka_utils.settings import *
from packet_utils.packet_processing import (
    process_car_telemetry,
    process_car_status,
    process_lap_data,
)

UDP_IP = "0.0.0.0"
UDP_PORT = 20777


producer = initialize_kafka_producer(kafka_server)
create_topic(topic_name)


async def receive_game_packets():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    while True:
        data, addr = sock.recvfrom(4096)
        await forward_refined_data_to_topic(data)


async def forward_refined_data_to_topic(packet):
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
        else:
            pass

    except Exception as e:
        print(f"Error processing UDP packet: {e}")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(receive_game_packets())
        loop.run_forever()
    finally:
        producer.close()
        loop.close()
