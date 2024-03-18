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
from kafka_utils.producer import publish_messages_to_kafka_socket2
producer = initialize_kafka_producer(kafka_server)

race_data = {
  "raceData": {
    "driver": "Max",
    "speed": 227,
    "throttle": 0.0,
    "brake": 0.0,
    "engineRPM": 10349,
    "gear": 6,
    "drs": 0,
    "tyresInnerTemperature_left_top": 87,
    "tyresInnerTemperature_right_top": 87,
    "tyresInnerTemperature_left_bottom": 87,
    "tyresInnerTemperature_right_bottom": 87
  }
}

create_topic(topic_name)


# 
publish_messages_to_kafka_socket2(producer, topic_name, race_data)
print('test')






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

# if __name__ == "__main__":
