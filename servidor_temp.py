from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

def generate_temperature_data():
    timestamp = datetime.now().isoformat()
    temperature = round(random.uniform(15.0, 35.0), 2)  # Genera una temperatura entre 15 y 35 grados
    return {"timestamp": timestamp, "temperature": temperature}

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        data = generate_temperature_data()
        producer.send('sensor_data', value=data)
        print(f"Sent data: {data}")
        time.sleep(5)  # Envía datos cada 5 segundos

if __name__ == "__main__":
    main()
