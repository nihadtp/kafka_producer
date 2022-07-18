import http.client
from kafka import KafkaProducer
import datetime
import logging
import json
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
file_name = os.path.join(dir_path, 'kafka_log.log')
date_frmt = "%d-%b-%Y"
current_time = datetime.date.today()

## Script for cron job. This script runs periodically every day. This sends out latest data to Kafka.

headers = {
    'X-RapidAPI-Key': "11c40f05e3msh3c0494a6579ab1dp1e1b14jsn7c9e708aa936",
    'X-RapidAPI-Host': "latest-mutual-fund-nav.p.rapidapi.com"
    }

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers='localhost:9092',  max_request_size=1000000000)

def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(filename=file_name)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(astime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    conn = http.client.HTTPSConnection("latest-mutual-fund-nav.p.rapidapi.com")
    conn.request("GET", f"/fetchHistoricalNAV?Date={current_time.strftime(date_frmt)}", headers=headers)
    res = conn.getresponse()
    data = res.read()
    try:
        future = producer.send(topic='mutualFundTopic', value=data.decode("utf-8"))
        result = future.get(timeout=60)
        logger.info(f'Producer write succesfully acknowldged for the data for date {current_time.strftime(date_frmt)}')
    except Exception as e:
        logger.error(f"Something wrong with producer. Refer this {e}")
    
if __name__ == "__main__":
    main()