from kafka.admin import KafkaAdminClient, NewTopic
from .settings import *

def create_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_server)
        topic_config = {
            "cleanup.policy": "compact",
            "compression.type": "lz4",
            "retention.ms": "3600000",  
        }

        topic_metadata = admin_client.list_topics()
        topic_names = topic_metadata
        topic_exists = any(topic_name == topic for topic in topic_names)

        if not topic_exists:
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1, topic_configs=topic_config)
            admin_client.create_topics([new_topic], validate_only=False)
            print(f"Kafka topic '{topic_name}' created successfully.")
        else:
            print(f"Kafka topic '{topic_name}' already exists.")

    except Exception as e:
        print(f"Error creating Kafka topic '{topic_name}': {str(e)}")


