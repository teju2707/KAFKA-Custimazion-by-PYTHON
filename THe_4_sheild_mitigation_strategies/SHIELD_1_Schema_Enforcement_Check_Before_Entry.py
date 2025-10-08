from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Define the RULES (schema)
value_schema = {
    "type": "record",
    "name": "Product",
    "fields": [
        {"name": "name", "type": "string"},   # MUST have name
        {"name": "price", "type": "int"}      # MUST have price
    ]
}

# Create producer with schema checking
producer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}, default_value_schema=value_schema)

# Try to send GOOD data ✅
good_data = {"name": "iPhone", "price": 999}
producer.produce(topic='products', value=good_data)
print("✅ Sent!")

# Try to send BAD data ❌
bad_data = {"product": "iPhone", "cost": 999}  # Wrong fields!
producer.produce(topic='products', value=bad_data)
# 💥 ERROR! Schema validation fails!
# Message never enters Kafka!