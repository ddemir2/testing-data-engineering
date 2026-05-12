# publisher.py
import pandas as pd
import requests, json, time
from google.cloud import pubsub_v1
from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo

# Edit from Patrick :)

PROJECT_ID = 'de-project-bus-lightyear'
TOPIC_ID   = 'bc_test'
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

start_time = time.time()
file = open("breadcrumbs_2026-05-09.log", 'r')
breadcrumb_count = 0
for line in file:
    breadcrumb_count += 1
    data = json.loads(line)
    payload = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_path, payload)


final_time = time.time() # record end of entire publisher run


# send sentinel message
# create timestamp string
timestamp = datetime.fromtimestamp(final_time, tz=ZoneInfo("America/Los_Angeles"))
todays_day_of_month = timestamp.strftime("%d")
todays_month_name   = timestamp.strftime("%b").upper()
todays_year         = timestamp.strftime("%Y")
todays_hour         = timestamp.strftime("%H")
todays_minute       = timestamp.strftime("%M")
todays_second       = timestamp.strftime("%S")
sentinel_timestamp  = todays_day_of_month + todays_month_name + todays_year + ': ' + todays_hour + ': ' + todays_minute + ': ' + todays_second
# create sentinel record
sentinel_data =  {
        'EVENT_NO_TRIP': 0, 'EVENT_NO_STOP': 0,
        'OPD_DATE': sentinel_timestamp, 'VEHICLE_ID': 0, 'METERS': breadcrumb_count,
        'ACT_TIME': 0.0, 'GPS_LONGITUDE': 0.0, 'GPS_LATITUDE': 0.0,
        'GPS_SATELLITES': 0.0, 'GPS_HDOP': 0.0
        }
# package sentinel payload
sentinel_payload = json.dumps(sentinel_data).encode('utf-8')
sentinel_future = publisher.publish(topic_path, sentinel_payload)

# wait for sentinel message to be sent
sentinel_future.result()

#print(f'\nSentinel message has been sent: {sentinel_future}')
print(f'{sentinel_data}')

# calculate summary statistics
wall_time = final_time - start_time
#received_vehicles = len(dazzle_list) - len(failed_ids)
throughput_rate = float(breadcrumb_count) / wall_time


# print summary statistics
print(f'\n\nSummary Statistics:\n---------------------------------------')
print(f'Begin Timestamp: {time.ctime(start_time)}')
print(f'Breacrumbs published: {breadcrumb_count}')
#print(f'Vehicles with available records: {received_vehicles}')
print(f'Wall time: {wall_time:.3f}s')
print(f'Throughput: {throughput_rate:.3f} breadcrumbs per second')
print(f'End Timestamp: {time.ctime(final_time)}')
