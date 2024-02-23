#!/usr/bin/env python3
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.write_api import SYNCHRONOUS
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import json
import os

# InfluxDB connection settings
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDG_BUCKET = os.getenv("INFLUXDB_BUCKET", "flight-recos")

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "decorated-recos")


def populate_influxdb_from_kafka():
    # Create an InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    # Create a Write API instance
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Create a Kafka consumer
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "reco-writers",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(
                        f"Consumer reached end of partition {msg.topic()} [{msg.partition()}]"
                    )
                elif msg.error():
                    # Error
                    print(f"Error: {msg.error()}")
                continue

            json_data = json.loads(msg.value().decode("utf-8"))
            print("loaded: ", json_data)
            # current_date = datetime.now()
            # search_time = json_data["search_time"]
            # timestamp = datetime.combine(current_date, datetime.strptime(search_time, "%H:%M:%S").time())
            # timestamp = current_date
            # timestamp_string = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            for reco in json_data["recos"]:
                influx_point = (
                    Point("decorated_recos")
                    .tag("search_id", json_data["search_id"])
                    .tag("search_country", json_data["search_country"])
                    .tag("OnD", json_data["OnD"])
                    .tag("trip_type", json_data["trip_type"])
                    .tag("main_airline", reco["main_marketing_airline"])
                    .field("price_EUR", reco["price_EUR"])
                    .field("advance_purchase", json_data["advance_purchase"])
                    .field("number_of_flights", reco["nb_of_flights"])
                    .time(
                        json_data["search_date"] + "T" + json_data["search_time"] + "Z"
                    )
                )

                # Write the data point to InfluxDB
                write_api.write(bucket=INFLUXDG_BUCKET, record=influx_point)

    except KeyboardInterrupt:
        consumer.close()

    finally:
        consumer.close()


if __name__ == "__main__":
    populate_influxdb_from_kafka()
