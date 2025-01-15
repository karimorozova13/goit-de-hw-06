from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Create Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

my_name = 'kariMo'

building_sensors_topic = f'{my_name}_building_sensors'
alert_topic = f'{my_name}_alert_topic'

num_partitions = 1
replication_factor = 1

new_building_sensors_topic = NewTopic(name=building_sensors_topic, num_partitions=num_partitions, replication_factor=replication_factor)
new_alert_topic = NewTopic(name=alert_topic, num_partitions=num_partitions, replication_factor=replication_factor)

try:
    admin_client.create_topics(new_topics=[new_building_sensors_topic, new_alert_topic], validate_only=False)
    print(f"Topics '{building_sensors_topic}', '{alert_topic}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

for topic in admin_client.list_topics():
    if "my_name" in topic:
        print(topic)

# Close client
admin_client.close()