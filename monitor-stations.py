#! /usr/bin/env python3
import json
from kafka import KafkaConsumer

contracts = {}
consumer = KafkaConsumer("empty-stations", bootstrap_servers=["localhost:9092", "localhost:9093"],
                         group_id="velib-empty-stations-manager")

for message in consumer:
    do_notify = False
    station = json.loads(message.value.decode())
    contract_name = station["contract_name"]
    station_number = station["number"]
    if contract_name not in contracts:
        if station["available_bikes"] == 0:
            contracts[contract_name] = {station_number: station}
            do_notify = True
    elif station_number not in contracts[contract_name]:
        if station["available_bikes"] == 0:
            contracts[contract_name][station_number] = station
            do_notify = True
    elif station_number in contracts[contract_name] and station["available_bikes"] > 0:
        del contracts[contract_name][station_number]

    if do_notify:
        print('La station {}, {} est vide. Il y a {} stations vides dans la ville.'.format(
            station["address"], station["contract_name"], len(contracts[contract_name])
        ))
