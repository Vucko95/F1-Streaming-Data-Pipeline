from kafka_utils.producer import publish_messages_to_kafka_socket

async def process_car_telemetry(packet, producer, topic_name):
    telemetry_entry = {
        "raceData": {
            "driver": "Max", 
            "speed": packet.carTelemetryData[19].speed, 
            "throttle": round(packet.carTelemetryData[19].throttle, 2) * 100,  
            "brake": round(packet.carTelemetryData[19].brake, 2) * 100, 
            "engineRPM": packet.carTelemetryData[19].engineRPM, 
            "gear": packet.carTelemetryData[19].gear, 
            "drs": packet.carTelemetryData[19].drs, 
            "tyresInnerTemperature_left_top": packet.carTelemetryData[19].tyresInnerTemperature[2], 
            "tyresInnerTemperature_right_top": packet.carTelemetryData[19].tyresInnerTemperature[3], 
            "tyresInnerTemperature_left_bottom": packet.carTelemetryData[19].tyresInnerTemperature[0], 
            "tyresInnerTemperature_right_bottom": packet.carTelemetryData[19].tyresInnerTemperature[1], 
        }
    }
    # print(telemetry_entry)
    await publish_messages_to_kafka_socket(producer, topic_name, telemetry_entry)

async def process_car_status(packet, producer, topic_name):
    ersStoreEnergy = packet.carStatusData[19].ersStoreEnergy
    max_ers_energy = 4000000.0
    battery_percentage = round((ersStoreEnergy / max_ers_energy) * 100, 1)

    car_status_data = {
        "carstatusData": {
            "fuel": round(packet.carStatusData[19].fuelInTank, 2), 
            "tyre_damage_left_bottom": packet.carStatusData[19].tyresWear[0],  
            "tyre_damage_right_bottom": packet.carStatusData[19].tyresWear[1], 
            "tyre_damage_left_top": packet.carStatusData[19].tyresWear[2], 
            "tyre_damage_right_top": packet.carStatusData[19].tyresWear[3], 
            "battery_percentage": battery_percentage, 
            "frontLeftWingDamage": packet.carStatusData[19].frontLeftWingDamage, 
            "frontRightWingDamage": packet.carStatusData[19].frontRightWingDamage, 
            "rearWingDamage": packet.carStatusData[19].rearWingDamage, 
        }
    }
    # print(car_status_data)
    await publish_messages_to_kafka_socket(producer, topic_name, car_status_data)

async def process_lap_data(packet, producer, topic_name):
    currentLapTime = packet.lapData[19].currentLapTime
    carPosition = packet.lapData[19].carPosition
    bestLapSector1TimeInMS = packet.lapData[19].bestLapSector1TimeInMS if hasattr(packet.lapData[19], 'bestLapSector1TimeInMS') else 0
    bestLapSector2TimeInMS = packet.lapData[19].bestLapSector2TimeInMS if hasattr(packet.lapData[19], 'bestLapSector2TimeInMS') else 0
    bestLapSector3TimeInMS = packet.lapData[19].bestLapSector3TimeInMS if hasattr(packet.lapData[19], 'bestLapSector3TimeInMS') else 0
    sector1TimeInMS = packet.lapData[19].sector1TimeInMS if hasattr(packet.lapData[19], 'sector1TimeInMS') else 0
    sector2TimeInMS = packet.lapData[19].sector2TimeInMS if hasattr(packet.lapData[19], 'sector2TimeInMS') else 0
    sector3TimeInMS = packet.lapData[19].sector3TimeInMS if hasattr(packet.lapData[19], 'sector3TimeInMS') else 0

    bestLapSector1TimeInMS = round(bestLapSector1TimeInMS / 1000.0, 3)
    bestLapSector2TimeInMS = round(bestLapSector2TimeInMS / 1000.0, 3)
    bestLapSector3TimeInMS = round(bestLapSector3TimeInMS / 1000.0, 3)
    sector1TimeInSeconds = round(sector1TimeInMS / 1000.0, 3)
    sector2TimeInSeconds = round(sector2TimeInMS / 1000.0, 3)
    sector3TimeInSeconds = round(sector3TimeInMS / 1000.0, 3)
    car_lap_data = {
        "carlapdata": {
            "currentLapTime": currentLapTime, 
            "carPosition": carPosition, 
            "bestLapSector1TimeInMS": bestLapSector1TimeInMS, 
            "bestLapSector2TimeInMS": bestLapSector2TimeInMS, 
            "bestLapSector3TimeInMS": bestLapSector3TimeInMS, 
            "sector1TimeInSeconds": sector1TimeInSeconds, 
            "sector2TimeInSeconds": sector2TimeInSeconds, 
            "sector3TimeInSeconds": sector3TimeInSeconds, 
        }
    }
    print(car_lap_data)
    await publish_messages_to_kafka_socket(producer, topic_name, car_lap_data)

# Add similar functions for other packet types if needed
