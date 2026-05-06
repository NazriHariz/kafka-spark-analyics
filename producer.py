import json
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPIC

def create_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def load_data(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def enrich_transaction(transaction):
    transaction['transaction_id'] = str(uuid.uuid4()) # Overwrite existing transaction_id
    transaction['timestamp'] = datetime.now().isoformat() # Add the injestion timestamp
    return transaction

def main():
    producer = create_producer()
    data = load_data('data.json')
    
    for record in data:
        enriched_record = enrich_transaction(record)
        
        try:
            producer.send(KAFKA_TOPIC, enriched_record)
            print(f"Sent: {enriched_record}")
        except Exception as e:
            print(f"Error sending message: {e}")
        time.sleep(1)
    
    producer.flush()
    producer.close()

if __name__ == "__main__": 
    main()