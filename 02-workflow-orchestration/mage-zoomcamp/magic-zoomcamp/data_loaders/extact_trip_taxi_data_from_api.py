import io
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def read_csv_pandas(url):

    taxi_dtypes = {
                    'VendorID' : pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance':float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str, 
                    'PULocationID':pd.Int64Dtype(), 
                    'DOLocationID':pd.Int64Dtype(), 
                    'payment_type':pd.Int64Dtype(), 
                    'fare_amount':float, 
                    'extra':float, 
                    'mta_tax':float, 
                    'tip_amount':float, 
                    'tolls_amount':float, 
                    'improvement_surcharge':float, 
                    'total_amount':float, 
                    'congestion_surcharge':float

                  }
    
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


    return pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)

@data_loader
def load_data_from_api(*args, **kwargs):

    urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz']

    
    dataframes = [read_csv_pandas(url) for url in urls]

   
    print('SHAPE OF DATA IS {}'.format(pd.concat(dataframes,axis=0,ignore_index=True).shape))

    return pd.concat(dataframes,axis=0,ignore_index=True)

    




