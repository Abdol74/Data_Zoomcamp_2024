
--- CREATE EXTERNAL TABLE FOR GREEN TAXI DATA ----

CREATE OR REPLACE EXTERNAL TABLE `primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022_hw`(
VendorID INTEGER,
lpep_pickup_datetime TIMESTAMP,
lpep_dropoff_datetime TIMESTAMP,
store_and_fwd_flag STRING,
RatecodeID FLOAT64,
PULocationID INTEGER,
DOLocationID INTEGER,
passenger_count FLOAT64,
trip_distance FLOAT64,
fare_amount FLOAT64,
extra FLOAT64,
mta_tax FLOAT64,
tip_amount FLOAT64,
tolls_amount FLOAT64,
ehail_fee INTEGER,
improvement_surcharge FLOAT64,
total_amount FLOAT64,
payment_type FLOAT64,
trip_type FLOAT64,
congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://primeval-legacy-412013-gcp-bucket/ny_taxi_data_2022_hw/*']
);


SELECT * FROM primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022_hw
LIMIT 10;

-------------------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://primeval-legacy-412013-gcp-bucket/ny_taxi_data_2022/*']
);


SELECT * 
FROM `primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022`
LIMIT 10;


 drop table `primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022`;


 drop table `primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned`;

  -- CREATE MANAGED TABLE 

CREATE OR REPLACE TABLE `primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned` AS
SELECT * FROM primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022_hw;


------------------------------------------- QES 1 ------------------------------------ 

SELECT COUNT(*) 
FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned;

--- ANSWER QES 1 : 840402 ----


--------------------------------------------------- QES 2 ----------------------------------------------------------

--- COUNT DISTINCT AGAINST EXTERNAL TABLE ---

SELECT COUNT(DISTINCT PULocationID)
FROM primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022;

-----  QUERY AGAINST EXTERNAL WILL BE PROCESS 0 MB BECAUSE BIGQUERY CAN IDENTIFY IT BECAUSE TABLE IS NOT IN BIGQUERY ITSELF --- 

---- COUNT DISTINCT AGAINST MANAGED TABLE ---- 

SELECT COUNT(DISTINCT PULocationID)
FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned;

----- QUERY AGAINST MANAGED TABLE WILL PROCESS APPROXMIATELY 6.41 MB ------ 


-------------------------- QES 3 -------------------------------------------------------

SELECT COUNT(fare_amount) 
FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned
WHERE fare_amount = 0.0 ;

---------- ANSWER QES3 : 1622 ----------------------------------------------------------------


--------------------------------------------------------- QES 4 -----------------------------------------


--- HINT : WHERE CONDITION EXECUTED BEFORE THE ORDER BY CLAUSE IN SQL QUERY ----- 

CREATE OR REPLACE TABLE `primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID
AS
SELECT * FROM primeval-legacy-412013.google_BQ_demo_dataset.external_green_taxi_data_2022_hw;


--------------------------------------------------------------------------------------------------------------

------------------------------- QES 5 ----------------------------

-----------  QUERY AGAINST NON PARTITIONED TABLE -------
SELECT COUNT(DISTINCT PULocationID)
FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

------ QUERY AGAINST NON PARTITIONED_TABLE IS 12.82 MB ----

------ QUERY AGAINST PARTITIONED TABLE ------

SELECT COUNT(DISTINCT PULocationID)
FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-------- QUERY AGAINST PARTITIONED AND CLUSTERED TABLE IS 1.12 MB -------- 

------ qes 6 ---- 
----- GCP Bucket ---- 


------ qes 7 ---- 
--- False ----


------------ QES 8 -------- 

SELECT COUNT(*) FROM primeval-legacy-412013.google_BQ_demo_dataset.managed_green_taxi_data_2022_partitioned_clustered;


------ 0 MB estimation to process because BigQuery get the count of table from metadata about the table ----- 