from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('raw-events')

for message in consumer:
    try:
        # Parse full message
        data = json.loads(message.value)
        
        # 🧹 FILTER: Extract only what we need
        filtered_data = {
            'name': data.get('name'),
            'price': data.get('price')
        }
        
        # Check if required fields exist
        if not filtered_data['name'] or not filtered_data['price']:
            print("⏭️ Skipping: Missing required fields")
            continue
        
        # Process only the filtered data
        print(f"✅ Processing: {filtered_data}")
        process(filtered_data)
        
    except KeyError as e:
        # Field doesn't exist - skip it
        print(f"⏭️ Skipping: Missing field {e}")
        continue
        
    except Exception as e:
        # Other errors - send to DLQ
        print(f"💀 Error: {e}. Sending to DLQ...")
        send_to_dlq(message.value)