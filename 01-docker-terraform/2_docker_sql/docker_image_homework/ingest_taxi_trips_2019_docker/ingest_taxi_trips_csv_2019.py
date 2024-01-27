#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def download_with_curl(url, output_file):
    try:
       
        command = f'curl -kLSs {url} -o {output_file}'
        exit_code = os.system(command)

        if exit_code == 0:
            print(f"Download successful. Saved as {output_file}")
        else:
            print(f"Error during download. Curl command returned non-zero exit code {exit_code}")
    except Exception as e:
        print(f"Error: {e}")

    
def detect_csv_extension(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    return csv_name


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    url = params.url
    csv_name = detect_csv_extension(url=url)

    # step 1 : upload csv data
    # stpe 2: create the engine to object to connect with postgres db
    # step 3: read csv data into pandas dataframe 
    # step 4: transform over columns of pandas dataframe 
    # step 5: create the table using the structure of pandas dataframe 
    # step 6: iterate over pandas dataframe using chunk size to ingest data into table

    download_with_curl(url=url, output_file=csv_name)
    # os.system(f"curl -kLSs {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")


    df_create = pd.read_csv(csv_name, nrows=1)

    df_create.lpep_pickup_datetime = pd.to_datetime(df_create.lpep_pickup_datetime)
    df_create.lpep_dropoff_datetime = pd.to_datetime(df_create.lpep_dropoff_datetime)

    df_create.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df = pd.read_csv(csv_name, chunksize=10000, iterator=True)

    while True:
        try:
            t_start = time()
            df_chunk = next(df)
            df_chunk.lpep_pickup_datetime = pd.to_datetime(df_chunk.lpep_pickup_datetime)
            df_chunk.lpep_dropoff_datetime = pd.to_datetime(df_chunk.lpep_dropoff_datetime)
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user',required=True,help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db_name', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(params=args)

