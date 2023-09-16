from confluent_kafka.admin import AdminClient, NewTopic

def create_kafka_topic(topic_name):
    try:
        kafka_server = 'localhost:29092'
        admin_client = AdminClient({"bootstrap.servers": kafka_server})

        topic_config = {
            "cleanup.policy": "compact",
            "compression.type": "lz4",
            "retention.ms": "3600000",  # 1 hour retention
        }

        topic_exists = topic_name in admin_client.list_topics().topics

        if not topic_exists:
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1, config=topic_config)
            admin_client.create_topics([new_topic])
            print(f"Kafka topic '{topic_name}' created successfully.")
        else:
            print(f"Kafka topic '{topic_name}' already exists.")
    
    except Exception as e:
        print(f"Error creating Kafka topic '{topic_name}': {str(e)}")

        