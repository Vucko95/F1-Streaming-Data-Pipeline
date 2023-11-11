import json
import socket
import asyncio
from f1_2020_telemetry.packets import *
from kafka import KafkaProducer
import json, time,random , string
from kafka import KafkaProducer
import asyncio
from kafka_utils.create_topics import create_kafka_topic


from kafka_utils.producer import initialize_kafka_producer,publish_messages_to_kafka,read_json_file,publish_messages_to_kafka_socket

topic_name = "F1Topic"
kafka_server = 'localhost:29092'
producer = initialize_kafka_producer(kafka_server)

async def process_telemetry_packet(data):
    try:
        packet = unpack_udp_packet(data)
        packet_id = packet.header.packetId
        if packet_id == PacketID.CAR_TELEMETRY:
            driver_telemetry_all = packet.carTelemetryData[19]
            speed = packet.carTelemetryData[19].speed
            throttle = round(packet.carTelemetryData[19].throttle, 2) * 100
            brake = round(packet.carTelemetryData[19].brake, 2) * 100
            engineRPM = packet.carTelemetryData[19].engineRPM 
            gear = packet.carTelemetryData[19].gear 
            drs = packet.carTelemetryData[19].drs 
            tyresInnerTemperature_left_top = packet.carTelemetryData[19].tyresInnerTemperature[2] 
            tyresInnerTemperature_right_top = packet.carTelemetryData[19].tyresInnerTemperature[3] 
            tyresInnerTemperature_left_bottom = packet.carTelemetryData[19].tyresInnerTemperature[0] 
            tyresInnerTemperature_right_bottom = packet.carTelemetryData[19].tyresInnerTemperature[1] 
    
            telemetry_entry = {
                "raceData": {
                    "driver": "Max", 
                    "speed": speed, 
                    "throttle": throttle,  
                    "brake": brake, 
                    "engineRPM": engineRPM, 
                    "gear": gear, 
                    "drs": drs, 
                    "tyresInnerTemperature_left_top": tyresInnerTemperature_left_top, 
                    "tyresInnerTemperature_right_top": tyresInnerTemperature_right_top, 
                    "tyresInnerTemperature_left_bottom": tyresInnerTemperature_left_bottom, 
                    "tyresInnerTemperature_right_bottom": tyresInnerTemperature_right_bottom, 
                }
            }
            # print(telemetry_entry)

            await publish_messages_to_kafka_socket(producer, topic_name, telemetry_entry)   

        elif packet_id == PacketID.CAR_STATUS:
            driver_car_status_all = packet.carStatusData[19]
            fuel = round(packet.carStatusData[19].fuelInTank, 2)
            tyre_damage_left_bottom = packet.carStatusData[19].tyresWear[0]
            tyre_damage_right_bottom = packet.carStatusData[19].tyresWear[1]
            tyre_damage_left_top = packet.carStatusData[19].tyresWear[2]
            tyre_damage_right_top = packet.carStatusData[19].tyresWear[3]
            ersStoreEnergy = packet.carStatusData[19].ersStoreEnergy
            frontLeftWingDamage = packet.carStatusData[19].frontLeftWingDamage
            frontRightWingDamage = packet.carStatusData[19].frontRightWingDamage
            rearWingDamage = packet.carStatusData[19].rearWingDamage
            max_ers_energy = 4000000.0
            battery_percentage = round((ersStoreEnergy / max_ers_energy) * 100, 1)
            # print(f"Battery Percentage: {battery_percentage}%")
            car_status_data = {
                "carstatusData": {
                    "fuel": fuel, 
                    "tyre_damage_left_bottom": tyre_damage_left_bottom,  
                    "tyre_damage_right_bottom": tyre_damage_right_bottom, 
                    "tyre_damage_left_top": tyre_damage_left_top, 
                    "tyre_damage_right_top": tyre_damage_right_top, 
                    "battery_percentage": battery_percentage, 
                    "frontLeftWingDamage": frontLeftWingDamage, 
                    "frontRightWingDamage": frontRightWingDamage, 
                    "rearWingDamage": rearWingDamage, 
                }
            }
            print(car_status_data)

            await publish_messages_to_kafka_socket(producer, topic_name, car_status_data)  
        elif packet_id == PacketID.LAP_DATA:
            # driver_car_status_all = packet.carStatusData[19]
            print(packet)
            print(packet)

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
