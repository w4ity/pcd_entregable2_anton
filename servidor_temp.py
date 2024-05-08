from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

class Sistema:
    def __init__(self, topic):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def iniciar_consumicion(self):
        self.generate_and_send_messages()

    def generate_fake_temperature(self):
        return round(random.uniform(20, 30), 2)

    def get_current_timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def generate_message(self):
        temperatura = self.generate_fake_temperature()
        timestamp = self.get_current_timestamp()
        return {"Timestamp": timestamp, "Temperatura": temperatura}

    def generate_and_send_messages(self):
        message_count = 0
        while True:  # Esto mantendrá la ejecución infinita
            message = self.generate_message()
            self.producer.send(self.topic, value=message)
            print(f'Message {message_count}: Timestamp: {message["Timestamp"]}, Temperatura: {message["Temperatura"]}')
            message_count += 1
            time.sleep(5)

if __name__ == "__main__":
    consumer = Consumer('nflx')
    consumer.iniciar_consumicion()
