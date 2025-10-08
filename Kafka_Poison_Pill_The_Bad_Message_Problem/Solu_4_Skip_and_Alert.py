from kafka import KafkaConsumer
import logging

consumer = KafkaConsumer('orders')

for message in consumer:
    try:
        process_order(message.value)
        
    except Exception as e:
        # Log the error
        logging.error(f"💀 Poison pill at offset {message.offset}")
        logging.error(f"Error: {e}")
        logging.error(f"Data: {message.value}")
        
        # Send alert (email, Slack, etc.)
        send_alert(f"Poison pill found! Check logs!")
        
        # Skip and continue
        print("⏭️ Skipping bad message...")
        continue