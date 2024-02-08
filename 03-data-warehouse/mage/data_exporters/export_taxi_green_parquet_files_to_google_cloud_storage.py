import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


table_schema_green = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/terraform-deploy-to-gcp/my-creds.json'

bucket_name = 'primeval-legacy-412013-gcp-bucket'
project_id = 'primeval-legacy-412013'
table_name = 'ny_taxi_data_2022_hw'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    # table = table.cast(table_schema_green)

    pq.write_to_dataset(
        table=table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs,
        coerce_timestamps='ms'
    )

