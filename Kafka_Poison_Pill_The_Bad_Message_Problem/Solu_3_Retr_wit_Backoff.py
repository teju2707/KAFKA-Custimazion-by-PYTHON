import time

max_retries = 3
retry_count = 0

for message in consumer:
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            process_order(message.value)
            print("✅ Success!")
            break  # Success! Move to next message
            
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count  # Exponential backoff
            
            print(f"⚠️ Failed! Retry {retry_count}/{max_retries}")
            print(f"⏱️ Waiting {wait_time} seconds...")
            
            time.sleep(wait_time)
    
    if retry_count == max_retries:
        # Still failed! Skip it
        print("💀 Giving up on this message. Moving on!")
        send_to_dlq(message.value)