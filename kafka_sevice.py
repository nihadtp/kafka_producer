import itertools
import json
import pprint
from kafka import KafkaProducer
import asyncio
import httpx
from typing import List
import datetime
import logging

#Data\e formsat to the API request - DD-MMM-YYYY (01-Jan-2022)
date_frmt = "%d-%b-%Y"
current_time = datetime.date.today()
logging.root.setLevel(logging.INFO)

#Header Info
headers = {
    'X-RapidAPI-Key': "11c40f05e3msh3c0494a6579ab1dp1e1b14jsn7c9e708aa936",
    'X-RapidAPI-Host': "latest-mutual-fund-nav.p.rapidapi.com"
    }

#Creates a producer object with broker at localhost:9092
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers='localhost:9092',  max_request_size=1000000000)

#Method to to send requests and concurrently process the response concurrenetly as soon as they are recieved regardless of when the
#when the request was send. This helps more response data to be written to the Kafka
async def historic_new(date: str, client: httpx.AsyncClient):
    params = {'Date': date}
    logging.info(f'Requested for Mututal Fund data for the date {date}')
    try:
        r = await client.get("https://latest-mutual-fund-nav.p.rapidapi.com/fetchHistoricalNAV", params=params)
        data = r.read()
        url = r.url.path
        logging.info(f'Received Mututal Fund data for the date {date}')
    except BaseException as e:
        logging.error(type(e))
        logging.error(e)
        logging.info(f'No data for the data {date}')
        data = "[{\"error\": \"error\"}]".encode("utf-8")


    payload = data.decode("utf-8")

    #Encapsulating response inside a json structure includeing a metadata component and body. Out API reponse(payload) will inside 
    # the body object.
    data = {
        'metadata': {
            'time': datetime.datetime.now().strftime("%d-%m-%yyyy %H:%M:%S"),
            'source': url
        },
        'body': {
            'payload': json.loads(payload)
        }
    }
    logging.info(f'Sending the response for the date {date} to the Kafka server')
    future = producer.send(topic='mutualFundTopic', value=data)
    result = future.get(timeout=60)
    logging.info(f'Producer write succesfully acknowldged for the data for date {date}')
    return result

#Fetch the reponse for all the 5000 request asynchronously and write to Kafka
async def fetch_all(dates: List[str]) -> List[str]:
    timeout = httpx.Timeout(60.0, connect=60.0)
    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        return await asyncio.gather(*map(historic_new, dates, itertools.repeat(client)))

def get_time_diff(time: int) -> str:
        delta = current_time - datetime.timedelta(days=time)
        return delta.strftime(date_frmt)

if __name__ == "__main__":
    logging.info("Starting the Kafka Service")
    logging.info(f'Requesting mutual funds data for 5000 days from {current_time.strftime(date_frmt)}')

    #Capture all 5000 dates before today and store it as list.
    deltas = list(range(1,5000))
    date_times = list(map(get_time_diff, deltas))
    loop = asyncio.get_event_loop()
    t1 = datetime.datetime.now()
    result = loop.run_until_complete(fetch_all(date_times))
    t2 = datetime.datetime.now()
    pprint.pprint(result)
    print(str((t2-t1).seconds/60))
