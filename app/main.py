import json, time,random , string
from kafka import KafkaProducer
from kafka_utils.create_topics import create_kafka_topic
from risingwave_utils.create_source import create_source
from risingwave_utils.create_mt_view import create_materialized_view
from kafka_utils.producer import initialize_kafka_producer,publish_messages_to_kafka,read_json_file


topic_name = "F1Topic"
source_name = "f1_stream2"
view_name = "f1_lap_times"
kafka_server = 'localhost:29092'


producer = initialize_kafka_producer(kafka_server)

create_kafka_topic(topic_name)
# create_source(source_name)
# create_materialized_view(view_name,source_name)    

race_info_file = "race_data.json"

race_info_data = read_json_file(race_info_file)
publish_messages_to_kafka(producer, topic_name, race_info_data)
