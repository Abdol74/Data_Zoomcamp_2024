import io
import pandas as pd
import requests
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    #url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-12.csv.gz'

    urls = [
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-01.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-02.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-03.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-04.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-05.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-06.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-07.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-08.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-09.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-10.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-11.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-12.csv.gz'
    ]
    chunk_size = 50000
    
    for url in urls:
        #url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz'
        count = 0
        try:
            df = pd.read_csv(url, compression='gzip',chunksize=chunk_size)
            
        except Exception as e:
            df = pd.read_csv(url, compression='gzip',chunksize=chunk_size, engine='python', encoding='latin-1')

        for chunk in df:
            filename_part = os.path.basename(url).replace('.csv.gz', '')
            filename_part = filename_part+'_'+str(count)
            # print(filename_part)
            try:
                chunk['PULocationID'] = chunk['PULocationID'].fillna(0)
                chunk['DOLocationID'] = chunk['DOLocationID'].fillna(0)
            except Exception as e:
                chunk['PUlocationID'] = chunk['PUlocationID'].fillna(0)
                chunk['DOlocationID'] = chunk['DOlocationID'].fillna(0) 


            chunk['SR_Flag'] = chunk['SR_Flag'].fillna(0)
            try:
                chunk = chunk.rename(columns={'dropOff_datetime': 'dropoff_datetime'})
                chunk = chunk.rename(columns={'PUlocationID': 'PULocationID'})
                chunk = chunk.rename(columns={'DOlocationID': 'DOLocationID'})
            except Exception as e:
                chunk = chunk.rename(columns={'dropoff_datetime': 'dropoff_datetime'})
                chunk = chunk.rename(columns={'PULocationID': 'PULocationID'})
                chunk = chunk.rename(columns={'DOLocationID': 'DOLocationID'})
                
            chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropoff_datetime"])
            chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
            chunk.PULocationID = chunk.PULocationID.astype('int64')
            chunk.DOLocationID = chunk.DOLocationID.astype('int64')
            chunk.SR_Flag = chunk.SR_Flag.astype('int64')
            chunk.dispatching_base_num = chunk.dispatching_base_num.astype('string')
            chunk.Affiliated_base_number   = chunk.Affiliated_base_number.astype('string')
            export_data_to_google_cloud_storage(df=chunk,variable_one=filename_part)
            count = count +1
        # print(chunk.info)
        

    return 1


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'cloud_bucket_dbt'
    object_key = 'fhv_parquet/{}.parquet'.format(kwargs['variable_one'])

    # print(object_key)
    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
        coerce_timestamps='ms'
    )

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
