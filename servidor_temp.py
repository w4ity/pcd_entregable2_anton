# servidor_temp.py

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

class Producer:
    def __init__(self, topic, freq):
        self.topic = topic
        self.freq = int(freq)
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generate_temperature_data(self):
        timestamp = datetime.now().isoformat()
        temperature = round(random.uniform(15.0, 35.0), 2)  # Genera una temperatura entre 15 y 35 grados
        return {"timestamp": timestamp, "temperature": temperature}

    def start_producer(self):
        try:
            while True:
                data = self.generate_temperature_data()
                self.producer.send(self.topic, value=data)
                print(f"Sent data: {data}")
                time.sleep(self.freq)
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")
        finally:
            self.producer.close()

if __name__ == '__main__':
    topic = sys.argv[1]
    freq = sys.argv[2]

    producer = Producer(topic, freq)
    producer.start_producer()
