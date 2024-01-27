
------ Q1 ---------------
SELECT count(*)
FROM yellow_taxi_trips_2019 as T
WHERE 
DATE(T.lpep_pickup_datetime) = DATE('2019-09-18')
AND
DATE(T.lpep_dropoff_datetime) = DATE('2019-09-18')
;



-----------  Q2 -----------------------
SELECT DATE(T.lpep_pickup_datetime) as day, SUM(T.trip_distance) as total_distance
FROM yellow_taxi_trips_2019 as T
GROUP BY DATE(T.lpep_pickup_datetime)
ORDER BY total_distance DESC
LIMIT 1
;

SELECT DATE(T.lpep_pickup_datetime) as day
FROM yellow_taxi_trips_2019 as T
WHERE T.trip_distance = (
							SELECT MAX(trip_distance)
							FROM yellow_taxi_trips_2019
						)
;




------------------ Q3 ----------------------------------
SELECT Z."Borough", SUM(T."total_amount") AS TOTAL_AMOUNT_PER_BOROUGH
FROM 
yellow_taxi_trips_2019 as T
INNER JOIN
zones as Z
ON 
T."PULocationID" = Z."LocationID"
GROUP BY Z."Borough"
HAVING SUM(T."total_amount") > 50000
ORDER BY TOTAL_AMOUNT_PER_BOROUGH DESC
LIMIT 3
;





---------------------- Q4 --------------------------

SELECT 
	Z_PICKUP."Zone" as pickup_zone,
	Z_DROPOFF."Zone" as dropoff_zone,
	SUM(T."tip_amount") as total_tip_amt_per_dropoff_zone
FROM yellow_taxi_trips_2019 as T
INNER JOIN 
zones as Z_PICKUP
ON 
T."PULocationID" = Z_PICKUP."LocationID"
INNER JOIN 
zones as Z_DROPOFF
ON
T."DOLocationID" = Z_DROPOFF."LocationID"
AND
Z_PICKUP."Zone" = 'Astoria'
AND
EXTRACT(MONTH FROM T."lpep_pickup_datetime") = 9
GROUP BY Z_PICKUP."Zone",Z_DROPOFF."Zone"
ORDER BY total_tip_amt_per_dropoff_zone DESC
LIMIT 1
;


----------------- Q5 ANOTHER ANSWER ---------------

SELECT 
	Z_PICKUP."Zone" as pickup_zone,
	Z_DROPOFF."Zone" as dropoff_zone
FROM yellow_taxi_trips_2019 as T
INNER JOIN 
zones as Z_PICKUP
ON 
T."PULocationID" = Z_PICKUP."LocationID"
INNER JOIN 
zones as Z_DROPOFF
ON
T."DOLocationID" = Z_DROPOFF."LocationID"
AND
Z_PICKUP."Zone" = 'Astoria'
AND
EXTRACT(MONTH FROM T."lpep_pickup_datetime") = 9
ORDER BY T."tip_amount" desc
LIMIT 1
;