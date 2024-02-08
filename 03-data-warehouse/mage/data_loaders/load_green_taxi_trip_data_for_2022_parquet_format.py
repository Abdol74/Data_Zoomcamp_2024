import io
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def read_parquet_into_dataframe(files):
    dataframes = []
    for file in files:
        dataframes.append(pd.read_parquet(file))
    
    concated_df = pd.concat(dataframes, axis=0, ignore_index=True)
    concated_df["lpep_pickup_datetime"] = pd.to_datetime(concated_df["lpep_pickup_datetime"])
    concated_df["lpep_dropoff_datetime"] = pd.to_datetime(concated_df["lpep_dropoff_datetime"])
    print(concated_df['lpep_pickup_datetime'].dtype)
    print(concated_df['lpep_dropoff_datetime'].dtype)
    # concated_df.passenger_count = concated_df.passenger_count.astype(int)
    # concated_df.RatecodeID = concated_df.RatecodeID.astype(int)
    # concated_df.payment_type = concated_df.payment_type.astype(int)
    # concated_df.trip_type = concated_df.trip_type.astype(int)

    return concated_df


@data_loader
def load_data_from_api(*args, **kwargs):

    url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    response = requests.get(url)


    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

    
        links = soup.find_all('a', href=True)


        parquet_links = [link['href'] for link in links if re.search(r'green_tripdata_2022-\w+\.parquet$', link['href'], re.I)]

    df = read_parquet_into_dataframe(parquet_links)

    print(df.info())
        
    # for parquet_link in parquet_links:
    #     print(parquet_link)
    return df




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
