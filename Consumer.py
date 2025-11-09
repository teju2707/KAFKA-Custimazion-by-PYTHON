from kafka import KafkaConsumer
import json
import six
import sys
import time
import os
import http.client
import ssl

FILES_REST_API =  '********-****-****-****-****************.files.hdl.prod-us30.hanacloud.ondemand.com'   # The REST API endpoint for your data lake instance
CONTAINER = '********-****-****-****-****************'  # The instance ID of your data lake container
CRT_PATH = 'Certs/client.crt'  # The file path to the client certificate for your data lake instance
KEY_PATH = 'Certs/client.key'  # The file path to the client key for your data lake instance

def upload_file(msg):
    file_name= time.strftime("%Y-%m-%d-%H%M%S")+".csv"
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(certfile=CRT_PATH, keyfile=KEY_PATH)
    request_url = '/webhdfs/v1/bronze/'+ file_name + '?op=CREATE&data=true'
    request_headers = {
        'x-sap-filecontainer': CONTAINER,
        'Content-Type': 'application/octet-stream'
    }
    connection = http.client.HTTPSConnection(FILES_REST_API, port=443, context=context)
    connection.request(method="PUT", url=request_url, body=msg, headers=request_headers)
    response = connection.getresponse()
    print("File "+file_name+" created")
    response.close()    


if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

consumer = KafkaConsumer(
    'KafkaDemo',                             # Tutorial can be replaced by the topic name that you gave in the producer code
    bootstrap_servers = [
    'localhost:9092'
    ],
    auto_offset_reset='earliest',  
    enable_auto_commit=True,      
)


heading=False
max_cap=256 * 1024
buffer=0
msg=''
try:
    for message in consumer:
        if not heading:
            msg+="Id,Temperature,Timestamp\n"
            heading=True
        msg_str = message.value.decode('utf-8')
        msg_without_backslash = msg_str.replace('\\"', '"')
        json_string = msg_without_backslash.strip('"')
        json_obj = json.loads(json_string)

        
        row = [str(json_obj['ID']), str(json_obj['temperature']), str(json_obj['timestamp'])]
        csv_row = ','.join(row) + '\n'
        msg += csv_row
        buffer += len(message.value)
        if (buffer>=max_cap):
            upload_file(msg)
            buffer=0
            msg=''
            heading=False
except KeyboardInterrupt:
    print("Code stopped by User")
        
consumer.close()

