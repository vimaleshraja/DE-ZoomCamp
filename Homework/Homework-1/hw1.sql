-- Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
--- ANS- East Harlem North
SELECT
    z."Zone",
    SUM(g.fare_amount) AS total
FROM green_trip_data g
LEFT JOIN zones_catalog z
    ON g."PULocationID" = z."LocationID"
GROUP BY z."Zone"
ORDER BY total desc;

-- For the passengers picked up in the zone named "East Harlem North" 
-- in November 2025, which was the drop off zone that had the largest tip?

----ANS - Yorkville west
SELECT
    dz."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS total_tip
FROM green_trip_data g
JOIN zones_catalog pz
    ON g."PULocationID" = pz."LocationID"
JOIN zones_catalog dz
    ON g."DOLocationID" = dz."LocationID"
WHERE
    pz."Zone" = 'East Harlem North'
    AND g.lpep_pickup_datetime >= '2025-11-01'
    AND g.lpep_pickup_datetime < '2025-12-01'
GROUP BY dz."Zone"
ORDER BY total_tip DESC
LIMIT 1;


