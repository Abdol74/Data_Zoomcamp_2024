
---- QUESTION 0 -------------- 


CREATE MATERIALIZED VIEW latest_dropoff_time AS
WITH MAX_DROPOFF_TIME AS (SELECT 
MAX(tpep_dropoff_datetime) AS latest_dropoff_time
FROM trip_data)
SELECT 
	zone,
	tpep_dropoff_datetime
FROM 
MAX_DROPOFF_TIME,
trip_data INNER JOIN taxi_zone
ON trip_data.dolocationid = taxi_zone.location_id
WHERE trip_data.tpep_dropoff_datetime = MAX_DROPOFF_TIME.latest_dropoff_time;

----------------------------------------------------------------------------



------------------------------- QUESTION 1 --------------------------------


CREATE MATERIALIZED VIEW highest_avg_trip_time AS
WITH ZONES_TIME AS(
SELECT 
	pickup_zones.zone as pickup_zone,
	dropoff_zones.zone as dropoff_zone,
	MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)
	AS max_trip_time,
	MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime)
	AS min_trip_time,
	AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) 
	as avg_trip_time

FROM trip_data 
INNER JOIN taxi_zone as pickup_zones
ON trip_data.pulocationid = pickup_zones.location_id
INNER JOIN taxi_zone as dropoff_zones
ON trip_data.dolocationid = dropoff_zones.location_id
GROUP BY pickup_zones.zone, dropoff_zones.zone)
SELECT MAX(avg_trip_time) AS max_avg_trip_time
FROM ZONES_TIME;





CREATE MATERIALIZED VIEW all_avg_trip_time AS
SELECT 
	pickup_zones.zone as pickup_zone,
	dropoff_zones.zone as dropoff_zone,
	AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) 
	as avg_trip_time

FROM trip_data 
INNER JOIN taxi_zone as pickup_zones
ON trip_data.pulocationid = pickup_zones.location_id
INNER JOIN taxi_zone as dropoff_zones
ON trip_data.dolocationid = dropoff_zones.location_id
GROUP BY pickup_zones.zone, dropoff_zones.zone;


CREATE MATERIALIZED VIEW AS pair_taxi_zone_highest_avg_time
SELECT 
	all_avg_trip_time.pickup_zone,
	all_avg_trip_time.dropoff_zone,
	all_avg_trip_time.avg_trip_time
FROM all_avg_trip_time
INNER JOIN highest_avg_trip_time
ON all_avg_trip_time.avg_trip_time = highest_avg_trip_time.max_avg_trip_time;


ANSWER: 
 pickup_zone   | dropoff_zone | avg_trip_time
----------------+--------------+---------------
 Yorkville East | Steinway     | 23:59:33



-------------------------------------------------------------------------------------



------------------------------- QUESTION 2 ----------------------------------

CREATE MATERIALIZED VIEW AS number_trips_pair_taxi_zone_highest_avg_time
SELECT 
	COUNT(*) AS count_of_trips
FROM all_avg_trip_time
INNER JOIN highest_avg_trip_time
ON all_avg_trip_time.avg_trip_time = highest_avg_trip_time.max_avg_trip_time;

 count_of_trips
----------------
              1
---------------------------------------------------------------------------------

---------------------------- QUESTION 03 ------------------------------------ 

CREATE MATERIALIZED VIEW latest_pickup_trip AS
    SELECT
        max(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data;
		

CREATE MATERIALIZED VIEW top_3_busiest_zones_17_hour_before AS
SELECT 
	taxi_zone.zone as pickup_zone,
	count(*) as count_trips_per_pickup
FROM 
trip_data
INNER JOIN
latest_pickup_trip 
ON trip_data.tpep_pickup_datetime > (latest_pickup_trip.latest_pickup_time - INTERVAL '17' HOUR)
INNER JOIN taxi_zone
ON trip_data.pulocationid = taxi_zone.location_id
GROUP BY taxi_zone.zone
ORDER BY count_trips_per_pickup DESC 
LIMIT 3;



     pickup_zone     | count_trips_per_pickup
---------------------+------------------------
 LaGuardia Airport   |                     19
 JFK Airport         |                     17
 Lincoln Square East |                     17


---------------------------------------------------------------------------------