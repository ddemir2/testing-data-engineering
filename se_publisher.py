import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import requests
import pandas as pd
import requests, json, time
from google.cloud import pubsub_v1
from zoneinfo import ZoneInfo


#--Configuration-------------------------------------------------
PROJECT_ID = 'de-project-bus-lightyear'
TOPIC_ID   = 'se_topic'
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


#--extract Dazzle vehicles to python list------------------------
dazzle_df = pd.read_csv('VehicleGroupsIDs-NEW.csv')
dazzle_list = dazzle_df['Dazzle'].to_list()


#--Variables-----------------------------------------------------
#use for testing
#dazzle_list = list([2907, 2908, 3044, 3051, 3014, 3022])
all_vehicles = len(dazzle_list)
failed_requests = []
breadcrumb_count = 0
start_time     = time.time()    # start the clock


#--Publish Loop-----------------------------------------------------
for index, vehicle_num in enumerate(dazzle_list):
  try:
    print(f'Calling API for vehicle \#{vehicle_num} . . . ({index+1}/{all_vehicles})\tcurrent breadcrumb count: {breadcrumb_count}')
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_num}"
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    h2_tags = soup.find_all('h2')
    h1_tags = soup.find_all('h1')
    #print(f'Number of h2 tags: {len(h2_tags)}, current breadcrumb count: {breadcrumb_count}')

    for h1 in h1_tags:
        find_date = re.search(r'stop data for (\d\d\d\d-\d\d-\d\d)', h1.text)
        if find_date:
            service_date = datetime.strptime(find_date.group(1), '%Y-%m-%d').date()
            print(service_date)

    for h2 in h2_tags:
        attempted_match = re.search(r'TRIP (-?\d+)', h2.text)
        if attempted_match:
          table = h2.next_sibling
          rows = table.find_all('tr')
          for row in rows:
            cells = row.find_all('td')
            if len(cells) == 24:
              vehicle_number    = cells[0].get_text()
              leave_time        = cells[1].get_text()
              train             = cells[2].get_text()
              route_number      = cells[3].get_text()
              direction         = cells[4].get_text()
              service_key       = cells[5].get_text()
              trip_number       = int(attempted_match.group(1))
              #trip_number_2     = cells[6].get_text()
              stop_time         = cells[7].get_text()
              arrive_time       = cells[8].get_text()
              dwell             = cells[9].get_text()
              location_id       = cells[10].get_text()
              door              = cells[11].get_text()
              lift              = cells[12].get_text()
              ons               = cells[13].get_text()
              offs              = cells[14].get_text()
              estimated_load    = cells[15].get_text()
              maximum_speed     = cells[16].get_text()
              train_mileage     = cells[17].get_text()
              pattern_distance  = cells[18].get_text()
              location_distance = cells[19].get_text()
              GPS_latitude      = cells[20].get_text()
              GPS_longitude     = cells[21].get_text()
              data_source       = cells[22].get_text()
              schedule_status   = cells[23].get_text()

              record = {
                "vehicle_number": vehicle_number, "leave_time": leave_time,
                "train" : train,                  "route_number" : route_number,
                "direction" : direction,           "service_key" : service_key,
                "trip_number" : trip_number,
                "stop_time" : stop_time,           "arrive_time" : arrive_time,
                "dwell" : dwell,                   "location_id" : location_id,
                "door" : door,                     "lift" : lift,
                "ons" : ons,                       "offs" : offs,
                "estimated_load" : estimated_load, "maximum_speed" : maximum_speed,
                "train_mileage" : train_mileage,   "pattern_distance" : pattern_distance,
                "location_distance" : location_distance, "GPS_latitude" : GPS_latitude,
                "GPS_longitude" : GPS_longitude,   "data_source" : data_source,
                "schedule_status" : schedule_status, "service_date" : service_date
              }

              if breadcrum_count % 5000 == 0:
                  print(record)

              breadcrumb_count += 1
              payload = json.dumps(record).encode('utf-8')
              future = publisher.publish(topic_path, payload)

  except Exception as e:
    print(f'\t!!! Bad Request for vehicle #{vehicle_num}')
    failed_requests.append(vehicle_num)
    continue


#--Create and Send Sentinel---------------------------------------------
sentinel_data = {"sentinel" : True, "total_count" : breadcrumb_count}
sentinel_payload = json.dumps(sentinel_data).encode('utf-8')
sentinel_future = publisher.publish(topic_path, sentinel_payload)
print(f'Sent Sentinel Message: {sentinel_data}')
sentinel_result = sentinel_future.result()


#--Summary Stats---------------------------------------------------------
final_time = time.time()
elapsed_time = final_time - start_time
throughput = float(breadcrumb_count) / float(elapsed_time)
received_vehicles = all_vehicles - len(failed_requests)

print("\n\n")
print(f'Records published: {breadcrumb_count}')
print(f'Vehicles with available records: {received_vehicles}')
print(f'Begin Timestamp: {time.ctime(start_time)}')
print(f'End Timestamp: {time.ctime(final_time)}')
print(f'Elapsed time: {elapsed_time:.3f}')
print(f'Throughput: {throughput:.3f} records/sec')
