import csv
import json
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import time
WEATHER_DATA = os.getenv('WEATHERDATA', 'weatherdata')
KAFKA_SERVER = os.getenv('KAFKA_BOOSTRAP_SERVER', 'localhost:9092')

class CSVHandler:
    def __init__(self, csv_file):
        self.csv_file = csv_file
    
    def read_csv(self):
        data = []
        with open(self.csv_file, mode='r', newline='', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data
    
class ConvertToJson:
    def __init__(self, data):
        self.data = data

    def convertor(self):
        json_data = []
        for row in self.data:
            try:
                data_dict = {
                    "Formatted Date": row['Formatted Date'],
                    "Summary": row['Summary'],
                    "Precip Type": row['Precip Type'],
                    "Temperature (C)": float(row['Temperature (C)']),
                    "Apparent Temperature (C)": float(row['Apparent Temperature (C)']),
                    "Humidity": float(row['Humidity']),
                    "Wind Speed (km/h)": float(row['Wind Speed (km/h)']),
                    "Wind Bearing (degrees)": float(row['Wind Bearing (degrees)']),
                    "Visibility (km)": float(row['Visibility (km)']),
                    "Loud Cover": float(row['Loud Cover']),
                    "Pressure (millibars)": float(row['Pressure (millibars)']),
                    "Daily Summary": row['Daily Summary']
                }
                json_data.append(data_dict)
            except KeyError as e:
                print(f"KeyError: Missing key '{e}' in row: {row}")
            except ValueError as e:
                print(f"ValueError: Invalid value in row: {row}")
        return json_data

class KafkaProducer:
    def __init__(self, producer):
        self.producer = producer

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed due to error: {err}")
        else:
            print(f"Message delivered to topic: {msg.topic()}")

    def json_serializer(self, obj):
        return json.dumps(obj).encode('utf-8')

    def produce_data(self, topic, data, delay_sec=5):
        for item in data:
            self.producer.produce(
                topic,
                key=str(item['Formatted Date']),
                value=self.json_serializer(item),
                on_delivery=self.delivery_report,
            )
            self.producer.flush()
            time.sleep(delay_sec)

def main(producer, csv_file_path ):
    
    csv_handler = CSVHandler(csv_file_path)
    csv_data = csv_handler.read_csv()

    converter = ConvertToJson(csv_data)
    json_data = converter.convertor()

    kafka_data = KafkaProducer(producer)
    kafka_data.produce_data(WEATHER_DATA, json_data)

if __name__ == "__main__":
    try:

        csv_file_path = './data/weatherHistory.csv'

        producer_config = {
            'bootstrap.servers': KAFKA_SERVER,
            'error_cb': lambda err: print(f'Kafka error: {err}'),
        }

        producer = SerializingProducer(producer_config)
        main(producer,  csv_file_path)
    except Exception as e:
        print(f"Error: {e}")
