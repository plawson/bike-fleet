#! /usr/bin/env python3
import json
import time
import urllib.request
import argparse

# Run `pip install kafka-python` to install this package
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--apikey', required=True, help='provide your API key')
args = parser.parse_args()

url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(args.apikey)

producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093"])

empty_stations = []

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        do_send = False
        stations_key = "{}_{}".format(station["contract_name"], station["number"])
        if stations_key in empty_stations and station["available_bikes"] > 0:
            empty_stations.remove(stations_key)
            do_send = True
        elif station["available_bikes"] == 0 and stations_key not in empty_stations:
            empty_stations.append(stations_key)
            do_send = True
        if do_send:
            producer.send("empty-stations", json.dumps(station).encode(),
                          key=station["contract_name"].encode())
    print("Produced {} station records".format(len(stations)))
    time.sleep(1)
