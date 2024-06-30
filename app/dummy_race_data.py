from kafka_utils.settings import *
from kafka_utils.producer import initialize_kafka_producer
from kafka_utils.producer import publish_messages_to_kafka_socket2
from kafka_utils.create_topic import create_topic
import time


class RaceDataGenerator:
    def __init__(self, kafka_server, topic_name):
        self.producer = initialize_kafka_producer(kafka_server)
        self.topic_name = topic_name

    def generate_race_data(
        self, driver, speed, throttle, brake, engine_rpm, gear, drs, tires_temp
    ):
        race_data = {
            "raceData": {
                "driver": driver,
                "speed": speed,
                "throttle": throttle,
                "brake": brake,
                "engineRPM": engine_rpm,
                "gear": gear,
                "drs": drs,
                "tyresInnerTemperature_left_top": tires_temp,
                "tyresInnerTemperature_right_top": tires_temp,
                "tyresInnerTemperature_left_bottom": tires_temp,
                "tyresInnerTemperature_right_bottom": tires_temp,
            }
        }
        return race_data

    def generate_and_publish_data(self, num_iterations=33, initial_speed=113):
        for _ in range(num_iterations):
            initial_speed += 5

            race_data = self.generate_race_data(
                "STER", initial_speed, 0.0, 0.0, 10349, 6, 0, 87
            )
            publish_messages_to_kafka_socket2(self.producer, self.topic_name, race_data)
            time.sleep(0.04)


create_topic(topic_name)
data_generator = RaceDataGenerator(kafka_server, topic_name)
data_generator.generate_and_publish_data()
