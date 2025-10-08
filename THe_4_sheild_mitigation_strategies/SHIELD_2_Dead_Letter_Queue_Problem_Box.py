from kafka import KafkaConsumer, KafkaProducer
import json

# Main consumer
consumer = KafkaConsumer('orders', group_id='processors')

# DLQ producer (for bad messages)
dlq_producer = KafkaProducer()

for message in consumer:
    try:
        # Try to process
        order = json.loads(message.value)
        
        # Validate
        if 'name' not in order:
            raise ValueError("Missing name!")
        
        # Process
        print(f"✅ Processed: {order['name']}")
        
    except Exception as e:
        # Can't process! Send to DLQ!
        print(f"💀 Bad message! Sending to DLQ...")
        
        dlq_producer.send(
            'dead-letter-queue',  # ← Special topic
            key=message.key,
            value=message.value,
            headers=[
                ('error', str(e).encode()),  # Why it failed
                ('original_topic', b'orders'),
                ('timestamp', str(message.timestamp).encode())
            ]
        )
        
        # Continue to next message! ➡️
        print("⏭️ Moving on...")
        continue