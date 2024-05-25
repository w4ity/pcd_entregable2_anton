from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import sys

class Producer:
    def __init__(self, topic):
        self.topic = topic
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
                print(f"Datos enviados: {data}")
                time.sleep(5)
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")
        finally:
            self.producer.close()

if __name__ == '__main__':
    # Parámetros para el tópico 
    topic = 'temp'
    producer = Producer(topic)
    producer.start_producer()