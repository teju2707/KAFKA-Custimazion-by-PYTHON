from kafka import KafkaProducer
import json
from datetime import datetime
import six
import sys
import random

# Compatibility fix for Kafka module with Python 3.12 and above
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

max_cap = 512 * 1024  # Maximum buffer size in bytes
buffer_size = 0
buffer = []
id = 1

while buffer_size < max_cap:
    temperature = round(random.uniform(-20.0, 40.0), 2)  
    timestamp = datetime.now().isoformat()  
    data = {
        'ID': str(id),
        'temperature': str(temperature),
        'timestamp': str(timestamp)
    }
    message = json.dumps(data)
    buffer.append(message)
    buffer_size += len(message.encode('utf-8'))
    id += 1

# Send buffered messages to the designated Kafka topic
for msg in buffer:
    producer.send('KafkaDemo', value=msg)  # 'KafkaDemo' is the Kafka topic name

producer.close()
print("Data Successfully Sent")
