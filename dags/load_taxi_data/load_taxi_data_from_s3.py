import boto3
from io import BytesIO, StringIO
import pandas as pd
from .db_helpers import postgres
from botocore import UNSIGNED
from botocore.client import Config


def copy_mapping_file_to_pg():
    with open('dags/load_taxi_data/taxi_zone_mapping.csv', 'r') as file:
        with postgres('etl_database') as c:
            c.copy_expert(f"""COPY raw_data.location_mapping FROM STDIN WITH (FORMAT CSV, HEADER)""", file)


def load_s3_data_to_pg(taxi_type='yellow', bucket='nyc-tlc'):
    """load taxi data from public s3 bucket, accepts a filter for the taxi type"""
    if taxi_type == 'yellow':
        cols = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'PULocationID',
                'DOLocationID']

    with postgres('etl_database') as cursor:
        cursor.execute(f"select key from utils.s3_incremental where _table = '{taxi_type}_taxi_trips'")
        existing_keys = [x[0] for x in cursor.fetchall()]

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    objects = s3.list_objects(Bucket=bucket)

    keys = [x for x in objects['Contents'] if
            taxi_type in x['Key'] and '2019' in x['Key'] and x['Key'] not in existing_keys]

    for key in keys:
        print(key, flush=True)
        obj = s3.get_object(Bucket=bucket, Key=key['Key'])
        sio = StringIO()
        sio.write(pd.read_csv(BytesIO(obj['Body'].read()))[cols].to_csv(index=False, header=False, sep=','))
        sio.seek(0)

        with postgres('etl_database') as c:
            c.copy_expert(f"""COPY raw_data.{taxi_type}_taxi_trips FROM STDIN WITH (FORMAT CSV)""", sio)

            c.execute("""INSERT INTO utils.s3_incremental VALUES(%s,%s,%s)""",
                      (bucket, key['Key'], f'{taxi_type}_taxi_trips'))
