# analysis.py
# Paste your analysis.py code here, please.
import requests, json, time
from google.cloud import pubsub_v1
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import logging
import pandas as pd
import os
import psycopg2 #1 add
from dotenv import load_dotenv #2 add
from sqlalchemy import create_engine #3 add

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


#---Constants---------------------------------------------------------------

home_dir = "/home/davvan/" # 4 switch
#home_dir = "/home/pawood/"
#home_dir = "/home/ddemir2/"

#---Data Structures---------------------------------------------------------
breadcrumb_count = 0
earliest_bc = None
latest_bc = None
wall_clock_time = None
unique_vehicles = set()
unique_trips = set()
expected_count = None
sentinel_time = None
unvalidated_batch_list = []
validate_count = 0


#---Helper Functions-------------------------------------------------------
def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid breadcrumb records to a dated JSON file.

    Parameters
    ----------
    invalid_records : list of dict
        Each dict should have a 'record' key (the original data)
        and a 'violations' key (list of assertion violation messages).
    run_date : str, optional
        Date string in YYYY-MM-DD format. Defaults to today.
    """
    if run_date is None:
        run_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')

    filename = home_dir + f"invalid_data/invalid_data_{run_date}.json"

    os.makedirs(home_dir + "invalid_data", exist_ok=True)
    invalid_records.to_json(filename, orient='records', lines=True, mode='a')


def validate_batch(unvalidated_batch_df):
  """
  Run all implemented validation assertions against a batch of breadcrumbs in a df

  Parameters
  ----------
  batch_df : df
    A df of breadcrumbs to validate.

  Returns
  -------
  validated_batch_df
        df containing only records which passed ALL assertions.
  violations_df
        df containing records which failed one or more assertions.
  """
  violations_df = pd.DataFrame(columns=['ASSERTION_FAILURE', 'EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])


  #---ASSERTION 1---[LIMIT]  GPS_LATITUDE must be non-null and in [-90, 90]-----
  lat_over_positive_90  = unvalidated_batch_df['GPS_LATITUDE'] > 90
  lat_under_negative_90 = unvalidated_batch_df['GPS_LATITUDE'] < -90
  null_lat = unvalidated_batch_df['GPS_LATITUDE'].isna()
  invalid_lat = unvalidated_batch_df[lat_over_positive_90 | lat_under_negative_90 | null_lat].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_lat.index) # remove offending rows from unvalidated_batch_df

  if not invalid_lat.empty:
      invalid_lat['ASSERTION_FAILURE'] = 'A1 [LIMIT]: GPS_LATITUDE is null or out of range [-90, 90]'
      if violations_df.empty:
          # If violations_df is initially empty, set it to the first set of violations
          violations_df = invalid_lat
      else:
          # Otherwise, concatenate with ignore_index=True to handle indices properly
          violations_df = pd.concat([violations_df, invalid_lat], ignore_index=True)

  #---ASSERTION 2---[LIMIT]  GPS_LONGITUDE must be non-null and in [-180, 180]-----
  long_over_positive_180  = unvalidated_batch_df['GPS_LONGITUDE'] > 180
  long_under_negative_180 = unvalidated_batch_df['GPS_LONGITUDE'] < -180
  null_long = unvalidated_batch_df['GPS_LONGITUDE'].isna()
  invalid_long = unvalidated_batch_df[long_over_positive_180 | long_under_negative_180 | null_long].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_long.index) # remove offending rows from unvalidated_batch_df

  if not invalid_long.empty:
      invalid_long['ASSERTION_FAILURE'] = 'A2: GPS_LONGITUDE is null or out of range [-180, 180]'
      if violations_df.empty:
          # If violations_df is initially empty, set it to the first
          violations_df = invalid_long
      else:
          # Otherwise, concatenate with ignore_index=True to handle indices properly
          violations_df = pd.concat([violations_df, invalid_long], ignore_index=True)

  #---ASSERTION 3---[EXISTENCE]  OPD_DATE must exist-----
  null_opd_date = unvalidated_batch_df['OPD_DATE'].isna()
  blank_opd_date = unvalidated_batch_df['OPD_DATE'] == ''
  invalid_opd_date = unvalidated_batch_df[null_opd_date | blank_opd_date].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_opd_date.index) # remove offending rows

  if not invalid_opd_date.empty:
      invalid_opd_date['ASSERTION_FAILURE'] = 'A3: OPD_DATE is null'
      if violations_df.empty:
          # If violations_df is initially empty, set it to the first
          violations_df = invalid_opd_date
      else:
          # Otherwise, concatenate with ignore_index=True to handle indices properly
          violations_df = pd.concat([violations_df, invalid_opd_date], ignore_index=True)

  #---ASSERTION 4---[LIMIT]  vehicle_id must non-null, greater than 0, and less than 9999999-----
  null_vehicle_id = unvalidated_batch_df['VEHICLE_ID'].isna()
  zero_or_less = unvalidated_batch_df['VEHICLE_ID'] <= 0
  over_limit   = unvalidated_batch_df['VEHICLE_ID'] >= 9999999
  invalid_vehicle_id = unvalidated_batch_df[null_vehicle_id | zero_or_less | over_limit].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_vehicle_id.index)

  if not invalid_vehicle_id.empty:
    invalid_vehicle_id['ASSERTION_FAILURE'] = 'A4: VEHICLE_ID is null or out of range'
    if violations_df.empty:
        # If violations_df is initially empty, set it to the first
        violations_df = invalid_vehicle_id
    else:
        # Otherwise, concatenate with ignore
        violations_df = pd.concat([violations_df, invalid_vehicle_id], ignore_index=True)

  #---ASSERTION 5---[INTER-RECORD]  A vehicle can't be on two different trips at once-----
  # A.K.A.: Summarize data by vehicle_ID, OPD_DATE, ACT_TIME. Each grouping may only have 1 unique EVENT_NO_TRIP
  summary_table = unvalidated_batch_df.groupby(['VEHICLE_ID', 'OPD_DATE', 'ACT_TIME'])['EVENT_NO_TRIP'].nunique().reset_index(name='unique_event_no_trip_count').copy()
  offending_vehicle_date_times = summary_table[summary_table['unique_event_no_trip_count'] > 1]
  for row_tuple in offending_vehicle_date_times.itertuples(index=False):
      invalid_vehicle  = row_tuple.VEHICLE_ID
      invalid_opd_date = row_tuple.OPD_DATE
      invalid_act_time = row_tuple.ACT_TIME
      invalid_trip     = unvalidated_batch_df[(unvalidated_batch_df['VEHICLE_ID'] == invalid_vehicle) & (unvalidated_batch_df['OPD_DATE'] == invalid_opd_date) & (unvalidated_batch_df['ACT_TIME'] == invalid_act_time)].copy()
      unvalidated_batch_df = unvalidated_batch_df.drop(invalid_trip.index)

      if not invalid_trip.empty:
        invalid_trip['ASSERTION_FAILURE'] = 'A5: A vehicle can\'t be on two different trips at once'
        if violations_df.empty:
          # If violations_df is initially empty, set it to the first
          violations_df = invalid_trip
        else:
          # Otherwise, concatenate with ignore
          violations_df = pd.concat([violations_df, invalid_trip], ignore_index=True)

  #---ASSERTION 6---[INTRA-RECORD / LIMIT]  GPS coordinates must fall within PDX area lat/long limits -----
  PDXAREA_LAT_MIN, PDXAREA_LAT_MAX =  45.0,  46.0
  PDXAREA_LON_MIN, PDXAREA_LON_MAX = -123.5, -122.0
  invalid_lat_min = unvalidated_batch_df['GPS_LATITUDE'] < PDXAREA_LAT_MIN
  invalid_lat_max = unvalidated_batch_df['GPS_LATITUDE'] > PDXAREA_LAT_MAX
  invalid_long_min = unvalidated_batch_df['GPS_LONGITUDE'] < PDXAREA_LON_MIN
  invalid_long_max = unvalidated_batch_df['GPS_LONGITUDE'] > PDXAREA_LON_MAX
  invalid_coordinates = unvalidated_batch_df[invalid_lat_min | invalid_lat_max | invalid_long_min | invalid_long_max].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_coordinates.index)

  if not invalid_coordinates.empty:
    invalid_coordinates['ASSERTION_FAILURE'] = 'A6: GPS coordinates are outside of PDX area'
    if violations_df.empty:
      # If violations_df is initially empty, set it to the first
      violations_df = invalid_coordinates
    else:
      # Otherwise, concatenate
      violations_df = pd.concat([violations_df, invalid_coordinates], ignore_index=True)


  #---ASSERTION 7---[EXISTENCE]   Following must be non-null: EVENT_NO_TRIP, EVENT_NO_STOP, METERS, ACT_TIME
  null_no_trip = unvalidated_batch_df['EVENT_NO_TRIP'].isna()
  null_no_stop = unvalidated_batch_df['EVENT_NO_STOP'].isna()
  null_meters  = unvalidated_batch_df['METERS'].isna()
  null_act_time = unvalidated_batch_df['ACT_TIME'].isna()
  invalid_fields = unvalidated_batch_df[null_no_trip | null_no_stop | null_meters | null_act_time].copy()
  unvalidated_batch_df = unvalidated_batch_df.drop(invalid_fields.index)

  if not invalid_fields.empty:
    invalid_fields['ASSERTION_FAILURE'] = 'A7: Expecting non-null fields, null value found'
    if violations_df.empty:
      # If violations_df is initially empty, set it to the first
      violations_df = invalid_fields
    else:
      # Otherwise, concat
      violations_df = pd.concat([violations_df, invalid_fields], ignore_index=True)

  #---At this point, invalid records have been fully removed from unvalidated_batch_df---
  if violations_df.empty:
    logging.info("No violations found")
  else:
    violations_df = violations_df.reset_index(drop=True)
    for row in violations_df.itertuples():
      logging.warning("VALIDATION VIOLATION - %s", row)

  #print(f'Violations_df size before truncation: {len(violations_df)}')
  validated_batch_df = unvalidated_batch_df.copy()
  #print(f'Violations_df size: {len(violations_df)}')
  #print(f'Validated_batch_df size: {len(validated_batch_df)}')
  return validated_batch_df, violations_df



def calc_breadcrumb_timestamp(opd_date, act_time):
        '''
        Each breadcrumb has it's datetime value split between two fields: OPD_DATE (string representing the correct day at midnight) >
        '''
        the_date = datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')
        the_date = the_date.date()
        time_elapsed = timedelta(seconds=act_time)
        proper_datetime=datetime.combine(the_date, datetime.min.time())+ time_elapsed
        return proper_datetime # return type is datetime object



def format_time(raw_timestamp):
    '''
    Generic function to convert a raw time.time() to a better readable time format
    in Pacific Time Zone (America/LosAngeles).
    '''

    if raw_timestamp is None:
        return "Not time"

    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')

def transform_data(valid_df):
    #Drop uneeded columns
    valid_df.drop(["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"], axis=1, inplace=True)

    #Create Timestamp column
    opd_date_series = pd.to_datetime(valid_df["OPD_DATE"], format='%d%b%Y:%H:%M:%S')
    act_time_series = pd.to_timedelta(valid_df["ACT_TIME"], unit='s')
    timestamp_series = opd_date_series + act_time_series
    timestamp_series.name = "timestamp"
    valid_df = valid_df.join(timestamp_series)
    valid_df.drop(["OPD_DATE", "ACT_TIME"], axis=1, inplace=True)

    #Create speed field
    delta_distance  = valid_df.groupby("EVENT_NO_TRIP")["METERS"].diff()
    delta_time = valid_df.groupby("EVENT_NO_TRIP")["timestamp"].diff().dt.total_seconds()
    speed_series = delta_distance / delta_time
    speed_series.name = "speed"
    valid_df = valid_df.join(speed_series)
    valid_df["speed"] = valid_df["speed"].fillna(0)

    #Rename Columns
    # 5 rename meters
    valid_df.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'VEHICLE_ID': 'vehicle_id', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', 'METERS': 'meters'}, inplace=True)

    return valid_df


def write_to_database(transformed_df): #6 add
  load_dotenv()
  log = logging.getLogger(__name__)
  TABLE_NAME = "breadcrumb"
  insert_count = 0

  DB_USERNAME = os.getenv('DB_USERNAME', 'DB_USERNAME_DEFAULT')
  DB_PASSWORD = os.getenv('DB_PASSWORD', 'DB_PASSWORD_DEFAULT')
  DB_HOST = os.getenv('DB_HOST', 'localhost')
  DB_PORT = os.getenv('DB_PORT', '5432')
  DB_NAME = os.getenv('DB_NAME', 'DB_NAME_DEFAULT')

  DATABASE_URL = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
  #print(f"Constructed DATABASE_URL: {DATABASE_URL}")
  #print(f"Attempting to insert into table: {TABLE_NAME} in database: {DB_NAME}")

  engine = None
  conn = None
  try:
    engine = create_engine(DATABASE_URL)
    #log.info("SQLAlchemy engine created.")
    with engine.connect() as connection:
        check_query = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{TABLE_NAME}';"
        table_exists = connection.execute(check_query).scalar()
        if not table_exists:
            log.warning(f"Table '{TABLE_NAME}' not found in 'public' schema. pandas.to_sql will attempt to create it.")

    #log.info(f"Calling pandas.to_sql for table '{TABLE_NAME}'...")
    transformed_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, schema='public')
    insert_count = len(transformed_df)
    #log.info(f"Successfully inserted {insert_count} records into 'public.{TABLE_NAME}' table.")

    #--- Direct verification after insertion ---
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, port=DB_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM public.{TABLE_NAME};")
    total_records = cur.fetchone()[0]
    cur.execute(f"SELECT * FROM public.{TABLE_NAME} LIMIT 1;")
    sample_record = cur.fetchone()

    print(f"\n--- Database Verification (from Python) ---")
    print(f"Total records in public.{TABLE_NAME} after insertion: {total_records}")
    print(f"Sample record from public.{TABLE_NAME}: {sample_record}")
    print(f"-------------------------------------------\n")

  except Exception as e:
    log.error(f"Error writing to database: {e}")
    if 'permission denied' in str(e).lower():
        log.error("This might be a permission issue. Ensure your database user has CREATE and WRITE privileges on the database and schema.")
  finally:
    if conn:
        conn.close()
        #log.info("psycopg2 connection closed.")
    if engine:
        engine.dispose()
        #log.info("SQLAlchemy engine disposed.")

  return insert_count



#---Congiguration----------------------------------------------------------
PROJECT_ID       = 'de-project-bus-lightyear'
SUBSCRIPTION_ID  = 'analysis_test'
subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


#---Callback Function------------------------------------------------------
def callback(message):
    global breadcrumb_count, unique_vehicles, unique_trips, earliest_bc, latest_bc, wall_clock_time
    global expected_count, sentinel_time, unvalidated_batch_list, validate_count
    message.ack()
    breadcrumb = json.loads(message.data.decode('utf-8')) # one breadcrumb

    # analysis happens here
    if breadcrumb['VEHICLE_ID'] == 0:
        # When sentinel recieve, get expected count
        expected_count = breadcrumb['METERS']
        sentinel_time = time.time()
    else:
            # Not sentinel so process data
        if wall_clock_time is None: #start timer when first breadcrumb recieved
            wall_clock_time = time.time()
            print(f"First breadcrumb received at {format_time(wall_clock_time)}")

        breadcrumb_count = breadcrumb_count + 1

        unvalidated_batch_list.append(breadcrumb)

        if breadcrumb_count % 100000 == 0:
            print(f"Collected {breadcrumb_count} so far")

        unique_vehicles.add(breadcrumb['VEHICLE_ID'])
        unique_trips.add(breadcrumb['EVENT_NO_TRIP'])

        raw_opd = breadcrumb['OPD_DATE']
        raw_act = breadcrumb['ACT_TIME']

        current_bc_time = calc_breadcrumb_timestamp(raw_opd, raw_act)

        if latest_bc is None or current_bc_time > latest_bc:
            latest_bc = current_bc_time
        if earliest_bc is None or current_bc_time < earliest_bc:
            earliest_bc = current_bc_time


    # After recieving Sentinel ensure that it hits expected count
    if expected_count is not None and breadcrumb_count == expected_count:
        elapsed_time = sentinel_time - wall_clock_time
        throughput = breadcrumb_count / elapsed_time

        #---Summary Statistics-----------------------------------------------------
        print("\nSentinel Recieved")
        print("Summary Statistics:")
        print(f"First message received: {format_time(wall_clock_time)}")
        print(f"Unique Vehicle IDs: {len(unique_vehicles)}")
        print(f"Earliest Breadcrumb from OPD and ACT: {earliest_bc}")
        print(f"Latest Breadcrumb from OPD and ACT: {latest_bc}")
        print(f"Unique Trip IDs: {len(unique_trips)}")
        print(f"Total Breadcrumbs Received: {breadcrumb_count}")
        print(f"Sentinel Received Time: {format_time(sentinel_time)}")
        print(f"Ellapsed Time: {elapsed_time:.3f}s")
        print(f"Throughput: {throughput:.3f} msg/s")

        final_unvalidated_df = pd.DataFrame(unvalidated_batch_list)
        print(f"Unvalidated DataFrame Shape: {final_unvalidated_df.shape}")
        validated_df, violations_df = validate_batch(final_unvalidated_df)
        print(f"Validated DataFrame Shape: {validated_df.shape}")
        print(f"Violations DataFrame Shape: {violations_df.shape}")

        #if not violations_df.empty:
        #    write_invalid_records(violations_df)

        transformed_df = transform_data(validated_df)
        print(transformed_df.head())
        write_to_database(transformed_df)

        #----Reset Data Structure(s)------------------------------------------------
        breadcrumb_count = 0
        expected_count = None
        unique_vehicles.clear()
        unique_trips.clear()
        earliest_bc = None
        latest_bc = None
        wall_clock_time = None
        sentinel_time = None
        validate_count = 0
        unvalidated_batch_list = []
        violations_df = None
        validated_df = None

#---Listening--------------------------------------------------------------
streaming_pull = subscriber.subscribe(sub_path, callback=callback)

current_time = format_time(time.time())
print(f"{current_time} - Listening for messages on {SUBSCRIPTION_ID} . . . .")

with subscriber:
        try:
                streaming_pull.result()
        except Exception:
                streaming_pull.cancel()
                streaming_pull.result()

