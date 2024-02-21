import io
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def read_csv_pandas(year,month):
    
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{}-{}.csv.gz'.format(year,month)
    print(url)
    # taxi_dtypes = {
    #                 'dispatching_base_num' : ,
    #                 'PULocationID':pd.Int64Dtype(), 
    #                 'DOLocationID':pd.Int64Dtype(), 
    #                 'SR_Flag':string
    #               }
    
    parse_dates = ['pickup_datetime', 'dropoff_datetime']
    df = pd.read_csv(url, sep=",", compression="gzip", parse_dates=parse_dates)

    return df

@data_loader
def load_data_from_api(*args, **kwargs):

    # urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
    #         'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
    #         'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz']

    years = [2019, 2020 ,2021]
    #url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{}-{}.csv.gz'.format(years,day)
    
    def read_partition(year):
        dataframes = []
        for i in range(12):
            month = '0'+str(i+1)
            month = month[-2:]
            read_csv_pandas(year,month)
        concated_df = pd.concat(dataframes, axis=0, ignore_index=True)
        return concated_df

    for year in years:
        return_list = []
        df = read_partition(year) 
        return_list.append(df)

    print(len(return_list))   
    # dataframes = [read_csv_pandas(url) for url in urls]

   
    # print('SHAPE OF DATA IS {}'.format(pd.concat(dataframes,axis=0,ignore_index=True).shape))

    # return pd.concat(dataframes,axis=0,ignore_index=True)
    return 1

    




