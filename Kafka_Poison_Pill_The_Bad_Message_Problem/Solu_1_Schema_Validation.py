# BEFORE (No validation - Bad!)
producer.send('orders', value=bad_data)  # ☠️ Might be poison!

# AFTER (With validation - Good!)
def validate_order(data):
    if 'name' not in data:
        return False
    if 'price' not in data:
        return False
    return True

if validate_order(order_data):
    producer.send('orders', value=order_data)  # ✅ Safe!
else:
    print("Invalid data! Not sending!")  # ❌ Reject bad data