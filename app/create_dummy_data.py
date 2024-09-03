# create_dummy_data.py
from kafka_utils.settings import kafka_server, topic_name
from kafka_utils.create_topic import create_topic
from test_data.dummy_race_data import RaceDataGenerator

def main():
    # Ensure topic creation before creating the data generator
    create_topic(topic_name)

    # Initialize and run the data generator
    data_generator = RaceDataGenerator(kafka_server, topic_name)
    data_generator.generate_and_publish_data()

if __name__ == "__main__":
    main()
