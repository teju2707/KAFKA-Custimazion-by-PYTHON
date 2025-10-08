from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('orders')
dlq_producer = KafkaProducer()

for message in consumer:
    try:
        # Try to process
        process_order(message.value)
        print("✅ Order processed!")
        
    except Exception as e:
        # Failed! Send to DLQ
        print(f"💀 Poison pill found! Sending to DLQ...")
        
        dlq_producer.send(
            'dead-letter-queue',  # ← Special topic for bad messages
            value=message.value
        )
        
        # Continue to next message (don't get stuck!)
        continue