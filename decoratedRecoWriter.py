#!/usr/bin/env python3
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import json
import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TS_DATABASE = os.getenv("TS_DATABASE_NAME", "flightdb")
TS_TABLE = os.getenv("TS_TABLE_NAME", "flight-recos")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "decorated-recos")

timestream_client = boto3.client(
    "timestream-write",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "reco-writers",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])


def populate_timestream_from_kafka():
    try:
        while True:
            for msg in consumer:
                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue

                json_data = json.loads(msg.value().decode("utf-8"))
                print("loaded: ", json_data)

                search_id = json_data["search_id"]
                search_country = json_data["search_country"]
                OnD = json_data["OnD"]
                trip_type = json_data["trip_type"]
                search_date = json_data["search_date"]
                search_time = json_data["search_time"]
                timestamp = datetime.strptime(
                    search_date + "T" + search_time, "%Y-%m-%dT%H:%M:%S"
                )

                for reco in json_data["recos"]:
                    dimensions = [
                        {"Name": "search_id", "Value": search_id},
                        {"Name": "search_country", "Value": search_country},
                        {"Name": "OnD", "Value": OnD},
                        {"Name": "trip_type", "Value": trip_type},
                        {
                            "Name": "main_airline",
                            "Value": reco["main_marketing_airline"],
                        },
                    ]
                    measures = [
                        {"Name": "price_EUR", "Value": str(reco["price_EUR"])},
                        {
                            "Name": "advance_purchase",
                            "Value": str(json_data["advance_purchase"]),
                        },
                        {
                            "Name": "number_of_flights",
                            "Value": str(reco["nb_of_flights"]),
                        },
                    ]

                    timestream_client.write_records(
                        DatabaseName=TS_DATABASE,
                        TableName=TS_TABLE,
                        Records=[
                            {
                                "Dimensions": dimensions,
                                "MeasureName": measure["Name"],
                                "MeasureValue": measure["Value"],
                                "Time": str(int(timestamp.timestamp() * 1000)),
                            }
                            for measure in measures
                        ],
                    )

    except KeyboardInterrupt:
        consumer.close()

    finally:
        consumer.close()


if __name__ == "__main__":
    populate_timestream_from_kafka()
