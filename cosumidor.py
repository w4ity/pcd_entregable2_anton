# consumer.py
from kafka import KafkaConsumer 
import json 
import statistics  # Importa el módulo statistics para cálculos estadísticos
from datetime import datetime  # Importa la clase datetime para manejar fechas y tiempos

# Singleton class
class Singleton:
    _instance = None

    @staticmethod
    def get_instance():
        if Singleton._instance is None:
            Singleton._instance = System()
        return Singleton._instance

# System class
class System:
    def __init__(self):
        self.observers = []  # Lista para almacenar los observadores

    def add_observer(self, observer):
        self.observers.append(observer)  # Agrega un observador a la lista

    def notify(self, data):
        for observer in self.observers:
            observer.update(data)  # Notifica a todos los observadores con los datos recibidos

# Observer class
class Observer:
    def update(self, data):
        pass  # Método a implementar por las clases que hereden de Observer

# ChainHandler class
class ChainHandler(Observer):
    def __init__(self):
        self.next_handler = None  # Inicializa el siguiente manejador en la cadena

    def set_next(self, handler):
        self.next_handler = handler  # Establece el siguiente manejador en la cadena

    def handle(self, data):
        if self.next_handler:
            self.next_handler.handle(data)  # Llama al siguiente manejador en la cadena si existe

# Statistics class
class Statistics(ChainHandler):
    def __init__(self, strategy):
        super().__init__()
        self.strategy = strategy  # Estrategia estadística a aplicar
        self.data = []  # Lista para almacenar los datos recibidos

    def update(self, data):
        self.data.insert(0, data)  # Inserta los datos al inicio de la lista
        self.handle(data)  # Llama al método handle para procesar los datos

    def handle(self, data):
        self.strategy.execute(self.data)  # Ejecuta la estrategia estadística
        super().handle(data)  # Llama al siguiente manejador en la cadena

# Strategy class
class Strategy:
    def execute(self, data):
        pass  # Método a implementar por las clases que hereden de Strategy

# MeanAndStdDevStrategy class
class MeanAndStdDevStrategy(Strategy):
    def execute(self, data):
        temperatures = [d['temperature'] for d in reversed(data)]  # Extrae las temperaturas en orden inverso
        mean = statistics.mean(temperatures)  # Calcula la media de las temperaturas
        print(f"Mean: {mean}")  # Imprime la media
        
        if len(temperatures) < 2:
            print("Not enough data points to calculate standard deviation")  # Imprime un mensaje si no hay suficientes datos
        else:
            try:
                stddev = statistics.stdev(temperatures)  # Calcula la desviación estándar de las temperaturas
                print(f"Standard deviation: {stddev}")  # Imprime la desviación estándar
            except statistics.StatisticsError as e:
                print(f"Error calculating standard deviation: {e}")  # Imprime el error si ocurre una excepción

# MaxMinStrategy class
class MaxMinStrategy(Strategy):
    def execute(self, data):
        temperatures = [d['temperature'] for d in data]  # Extrae las temperaturas
       
        if not temperatures:
            print("Not enough data points to calculate max and min")  # Imprime un mensaje si no hay suficientes datos
        else:
            try:
                max_value = max(temperatures)  # Calcula el valor máximo de las temperaturas
                min_value = min(temperatures)  # Calcula el valor mínimo de las temperaturas
                print(f"Max value: {max_value}, Min value: {min_value}")  # Imprime los valores máximo y mínimo
            except ValueError as e:
                print(f"Error calculating max and min: {e}")  # Imprime el error si ocurre una excepción

# QuartileStrategy class
class QuartileStrategy(Strategy):
    def execute(self, data):
        temperatures = [d['temperature'] for d in data]  # Extrae las temperaturas

        if len(temperatures) < 4:
            print("Not enough data points to calculate quartiles")  # Imprime un mensaje si no hay suficientes datos
        else:
            try:
                q1 = statistics.quantiles(temperatures, n=4)[0]  # Calcula el primer cuartil
                q3 = statistics.quantiles(temperatures, n=4)[-1]  # Calcula el tercer cuartil
                print(f"Q1: {q1}, Q3: {q3}")  # Imprime los valores del primer y tercer cuartil
            except statistics.StatisticsError as e:
                print(f"Error calculating quartiles: {e}")  # Imprime el error si ocurre una excepción

# simulate_data_reception function
def simulate_data_reception():
    system = Singleton.get_instance()  # Obtiene la instancia del sistema (Singleton)
    mean_stddev_strategy = MeanAndStdDevStrategy()  # Crea la estrategia de media y desviación estándar
    maxmin_strategy = MaxMinStrategy()  # Crea la estrategia de máximo y mínimo
    quartile_strategy = QuartileStrategy()  # Crea la estrategia de cuartiles

    statistics_handler = Statistics(mean_stddev_strategy)  # Crea el manejador de estadísticas para la estrategia de media y desviación estándar
    statistics_handler.set_next(Statistics(maxmin_strategy))  # Establece el siguiente manejador para la estrategia de máximo y mínimo
    statistics_handler.set_next(Statistics(quartile_strategy))  # Establece el siguiente manejador para la estrategia de cuartiles

    system.add_observer(statistics_handler)  # Agrega el manejador de estadísticas al sistema como observador

    consumer = KafkaConsumer(
        'temp',  # Nombre del topic de Kafka a consumir
        bootstrap_servers='localhost:9092',  # Servidor de Kafka
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserializador de los valores en JSON
    )

    try:
        for message in consumer:
            data = message.value  # Obtiene el valor del mensaje (datos en formato JSON)
            if 'temperature' in data:  # Verifica si los datos contienen la temperatura
                system.notify(data)  # Notifica al sistema con los datos recibidos
    except Exception as e:
        print(f"Error receiving data from Kafka: {e}")  # Imprime un mensaje de error si ocurre alguna excepción al recibir datos de Kafka
    finally:
        consumer.close()  # Cierra el consumidor de Kafka

if __name__ == '__main__':
    simulate_data_reception()  # Ejecuta la simulación de recepción de datos
