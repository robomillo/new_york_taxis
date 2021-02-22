DROP TABLE IF EXISTS datasets.historical_aggregates;
CREATE TABLE datasets.historical_aggregates AS (SELECT TO_CHAR(pickup_datetime, 'YYYY-MM') pick_up_month,
                                                       lm_pu.borough        AS             pick_up_borough,
                                                       lm_pu.zone           AS             pick_up_location,
                                                       lm_do.borough        AS             drop_off_borough,
                                                       lm_do.zone           AS             drop_off_location,
                                                       COUNT(*)             AS             total_trips,
                                                       COALESCE(SUM(passenger_count),0) AS             total_passengers
                                                FROM raw_data.yellow_taxi_trips ytt
                                                     LEFT JOIN raw_data.location_mapping lm_pu
                                                               ON lm_pu.location_id = ytt.pulocationid::INT
                                                     LEFT JOIN raw_data.location_mapping lm_do
                                                               ON lm_do.location_id = ytt.dolocationid::INT
                                                GROUP BY 1, 2, 3, 4, 5);

-- question 2.1 most popular (number of passengers) destination - zone dimension
DROP TABLE IF EXISTS datasets.popular_dest_zones_by_total_passengers;
CREATE TABLE datasets.popular_dest_zones_by_total_passengers AS (SELECT pick_up_month,
                                                                                    pick_up_location,
                                                                                    drop_off_location,
                                                                                    total_passengers,
                                                                                    RANK()
                                                                                    OVER (PARTITION BY pick_up_month, pick_up_location ORDER BY total_passengers DESC) AS total_passengers_rank
                                                                             FROM datasets.historical_aggregates
                                                                             WHERE pick_up_month LIKE '2019%');
-- questsion 2.2 most popular (number of rides) destination - borough dimension
DROP TABLE datasets.popular_dest_boroughs_by_ride_count;
CREATE TABLE datasets.popular_dest_boroughs_by_ride_count AS (WITH
                                                                  borough_aggregate AS (SELECT pick_up_month,
                                                                                               pick_up_borough,
                                                                                               drop_off_borough,
                                                                                               SUM(total_trips) total_trips
                                                                                        FROM datasets.historical_aggregates
                                                                                        GROUP BY 1, 2, 3)
                                                              SELECT pick_up_month,
                                                                     pick_up_borough,
                                                                     drop_off_borough,
                                                                     total_trips,
                                                                     RANK()
                                                                     OVER (PARTITION BY pick_up_month, pick_up_borough ORDER BY total_trips DESC NULLS LAST) AS total_trips_rank
                                                              FROM borough_aggregate
                                                              WHERE pick_up_month LIKE '2019%');


-- question 3 historical rankings

DROP TABLE IF EXISTS tmp.ranked_locations;
CREATE TABLE tmp.ranked_locations AS (SELECT *,
                                             RANK()
                                             OVER (PARTITION BY pick_up_month,pick_up_location ORDER BY total_trips DESC ) AS   total_trips_rank,
                                             RANK()
                                             OVER (PARTITION BY pick_up_month,pick_up_location ORDER BY total_passengers DESC ) total_passengers_rank
                                      FROM datasets.historical_aggregates
                                      WHERE pick_up_month LIKE '2019%');
DROP INDEX IF EXISTS rank_loc_index;
CREATE INDEX rank_loc_index ON tmp.ranked_locations (pick_up_month, pick_up_location, drop_off_location);

DROP TABLE IF EXISTS datasets.historical_rankings CASCADE;
CREATE TABLE datasets.historical_rankings AS (WITH
                                                  cte AS (SELECT re.pick_up_month,
                                                                 re.pick_up_location,
                                                                 re.drop_off_location,
                                                                 re.total_trips_rank,
                                                                 LAG(re.total_trips_rank)
                                                                 OVER (PARTITION BY pick_up_location,drop_off_location ORDER BY re.pick_up_month) =
                                                                 re.total_trips_rank AS filter
                                                          FROM tmp.ranked_locations re
                                                          ORDER BY re.pick_up_month, total_trips_rank)
                                              SELECT pick_up_month,
                                                     pick_up_location,
                                                     drop_off_location,
                                                     total_trips_rank
                                              FROM cte
                                              WHERE (filter = FALSE OR filter IS NULL));;


-- questions 4 current rankings
CREATE OR REPLACE VIEW datasets.current_month_ranked_trips AS
(
WITH
    cte AS (SELECT pick_up_location, total_trips_rank, MAX(pick_up_month) AS pick_up_month
            FROM datasets.historical_rankings

            GROUP BY 1, 2
            ORDER BY 2 DESC)
SELECT t.*
FROM datasets.historical_rankings t
     INNER JOIN cte ON t.pick_up_month = cte.pick_up_month AND t.pick_up_location = cte.pick_up_location AND
                       t.total_trips_rank = cte.total_trips_rank
ORDER BY pick_up_location, total_trips_rank);
