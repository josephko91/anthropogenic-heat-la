/*
Export 2016 congestion data into a table
*/
-- Create a table that will hold the results. Make sure it is in text format with fields separated by comma.
CREATE TABLE exports.congestion_data_2016 (
    link_id VARCHAR(50),
    link_type VARCHAR(20),
    date_and_time TIMESTAMP,
    agency VARCHAR(50),
    speed DOUBLE,
    volume DOUBLE,
    link_status VARCHAR(10),
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    dow int,
    hod int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert the query result into the table.
INSERT INTO exports.congestion_data_2016
SELECT  d.link_id                                                               AS link_id,
        d.link_type                                                             AS link_type,
        d.date_and_time                                                         AS date_and_time,
        d.agency                                                                AS agency,
        d.speed                                                                 AS speed,
        d.volume                                                                AS volume,
        d.link_status                                                           AS link_status,
        d.year                                                                  AS year,
        d.month                                                                 AS month,
        d.day                                                                   AS day,
        dayofweek(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles'))   AS dow,
        hour(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles'))        AS hod
FROM adms.congestion_data d
WHERE d.year = 2016;

-- Check the result.
SELECT *
from exports.temp;

-- Method 2: create table as
CREATE TABLE exports.congestion_data_2016 AS
SELECT *
FROM adms.congestion_data
WHERE year = 2016;

-- ===============================================

/*
Create 2016 hourly table
Repace dow with weekend column (1 = weekend, 0 = weekday)
*/
-- Create a table that will hold the results. Make sure it is in text format with fields separated by comma.
CREATE TABLE exports.jk_congestion_hr_2016 (
    link_id VARCHAR(50),
    month int,
    weekend int,
    hod int,
    avg_speed DOUBLE,
    total_volume DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert the query result into the table.
INSERT INTO exports.jk_congestion_hr_2016
SELECT  d.link_id                                       AS link_id,
        d.month                                         AS month,
        if(dayofweek(d.date_and_time) = 1 
            OR dayofweek(d.date_and_time) = 7, 1, 0)    AS weekend,
        hour(d.date_and_time)                           AS hod,
        avg(d.speed)                                    AS avg_speed,
        sum(d.volume)                                   AS total_volume
FROM adms.congestion_data d
WHERE d.link_status = 'OK'
    AND d.year = 2016 -- Note: we use `year` and `month` partition columns for performance improvements.
GROUP BY month, weekend, hod, d.link_id;

-- Check the result.
SELECT *
FROM exports.jk_congestion_hr_2016
WHERE link_id = '763517'
ORDER BY month, weekend, hod;

-- ===============================================

/*
Query number of fails vs. successes in an hour
*/
-- attempt 1: 
SELECT link_id, COUNT(*) as failed_hr
FROM adms.congestion_data
WHERE year = 2016 AND MONTH = 1 AND link_status = 'FAILED'
GROUP BY link_id, hour(date_and_time);

SELECT director, COUNT(*)
FROM Film
WHERE year > 2001
GROUP BY director;

-- attempt 2: one day of year
SELECT link_id,
    hour(date_and_time) AS hod,
    COUNT(*) AS total,
    SUM(if(link_status = 'OK', 1, 0)) AS success_count,
    SUM(if(link_status = 'FAILED', 1, 0)) AS fail_count
FROM adms.congestion_data
WHERE YEAR = 2016 AND MONTH = 1 AND DAY = 3
GROUP BY link_id, hod
ORDER BY link_id, hod
;

-- attempt 3: aggregate by month, weekend, hod
SELECT link_id,
    hour(date_and_time)                                 AS hod,
    month,
    if(dayofweek(date_and_time) = 1 
            OR dayofweek(date_and_time) = 7, 1, 0)      AS weekend,
    COUNT(*)                                            AS total,
    SUM(if(link_status = 'OK', 1, 0))                   AS success_count,
    SUM(if(link_status = 'FAILED', 1, 0))               AS fail_count
FROM adms.congestion_data
WHERE year = 2016 AND (month = 1 OR month = 2)
GROUP BY link_id, month, hod, weekend
ORDER BY link_id, month, hod, weekend
;

/*
Check the nub
*/

-- Check the hours of day for 01/01/2016
-- Only hours 21-23 appears in above query
SELECT COUNT(DISTINCT hour(date_and_time))
FROM adms.congestion_data
WHERE YEAR = 2016 AND MONTH = 1 AND DAY = 1;
-- NOTE: There seems to be only 3 hours (21-23) for 01/01/2016

-- ===============================================

-- We will be processing data for the year 2016. Let's count them just to get an idea of the data size.
SELECT count(*)
FROM adms.congestion_data
WHERE year = 2016;
-- Output: There are 6,484,886,482 records for the year of 2016
;

-- number of unique link_ids from congestion_data
SELECT count(*)
FROM (
    SELECT DISTINCT link_id
    FROM adms.congestion_data
    WHERE year = 2016
) d
-- Output = 16,720
;
-- number of unique link_ids from congestion_inventory
SELECT count(*)
FROM (
    SELECT DISTINCT link_id
    FROM adms.congestion_inventory
    WHERE year = 2016
) d
-- Output = 16,205
;

/*
Back of the envelope calculations:

- How many rows of data if aggregating to every hour of year?
(8,760 hrs) x (16,720 sensors) = 146,467,200 rows

- How many rows of data if aggregating by weekday/weekend and month?
(2 choices) x (12 months) x (16,720 sensors) = 401,280
*/

/*
Note: There is a mismatch between the count of unique link id's 
using congestion_data vs. congestion_inventory tables (year = 2016)
That's why we use years 2015-2017 below so that we don't miss an links (hopefully).
*/
-- How many distinct sensors are there in the network?
SELECT count(*)
FROM (
    SELECT DISTINCT link_id
    FROM adms.congestion_inventory
    WHERE year > 2014 AND year < 2018
) d
-- Output: 17,215
;

-- How many highway sensors are there?
SELECT count(*)
FROM (
    SELECT DISTINCT link_id
    FROM adms.congestion_inventory
    WHERE link_type = 'HIGHWAY' AND year > 2014 AND year < 2018
) d
-- Output: 4,647
;

-- How many local/arterial sensors are there?
SELECT count(*)
FROM (
    SELECT DISTINCT link_id
    FROM adms.congestion_inventory
    WHERE link_type = 'ARTERIAL'AND year > 2014 AND year < 2018
) d
-- Output: 12,568
;


/*
Checksum: 4,647 + 12,568 = 17,215? --> Yes
*/;

-- Get 2015-2017 congestion inventory table
SELECT *
FROM congestion_inventory
WHERE  year > 2014 AND year < 2018;

-- Let's choose a specific link to see why there are multiple records for the same link in inventory
SELECT * 
FROM congestion_inventory
WHERE  year > 2014 AND year < 2018 AND link_id = '822249'
-- We notice that the relevant link attributes are the same across duplicates
;

-- Get all distinct links from inventory
SELECT DISTINCT link_id, agency, city, link_type, on_street, 
    from_street, to_street, start_location_lng, start_location_lat,
    direction, postmile, num_lanes
FROM congestion_inventory;

/*
Issue: some links don't seem to have lat/lon values
Spot check links with issues to diagnose underlying problem.
*/
SELECT * 
FROM congestion_inventory
WHERE link_id = '1220146';

/*
Issue: data from highways seems to be missing in 2016 data
Check for data on 'SR-60' (link_id = '768444')
*/
SELECT * 
FROM congestion_data
WHERE year = 2016 AND month = 8 AND day=15 AND link_id = '768444';

/*
What is the fail rate of sensors in 2016?
*/
CREATE TABLE exports.jk_annual_failure_rate_2016 (
    link_id VARCHAR(50),
    n_success DOUBLE,
    n_failure DOUBLE,
    failure_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO exports.jk_annual_failure_rate_2016
SELECT
    link_id,
    sum(if(link_status = 'OK', 1, 0)) AS n_success,
    sum(if(link_status = 'FAILED', 1, 0)) AS n_failure,
    sum(if(link_status = 'FAILED', 1, 0))/(sum(if(link_status = 'FAILED', 1, 0))+sum(if(link_status = 'OK', 1, 0))) AS failure_rate
FROM adms.congestion_data
WHERE year = 2016
GROUP BY link_id;

/* old code
SELECT
    d.link_id,
    sum(d.success) AS n_success,
    sum(d.failure) AS n_failure,
    sum(d.failure)/(sum(d.failure)+sum(d.success)) AS failure_rate
FROM (
    SELECT 
        link_id,
        if(link_status = 'OK', 1, 0) AS success,
        if(link_status = 'OK', 0, 1) AS failure
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 8
) d
GROUP BY link_id;
*/

/*
What is the fail rate of sensors in 2017?
*/
;

CREATE TABLE exports.jk_annual_failure_rate_2017 (
    link_id VARCHAR(50),
    n_success DOUBLE,
    n_failure DOUBLE,
    failure_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
;

INSERT INTO exports.jk_annual_failure_rate_2017
SELECT
    link_id,
    sum(if(link_status = 'OK', 1, 0)) AS n_success,
    sum(if(link_status = 'FAILED', 1, 0)) AS n_failure,
    sum(if(link_status = 'FAILED', 1, 0))/(sum(if(link_status = 'FAILED', 1, 0))+sum(if(link_status = 'OK', 1, 0))) AS failure_rate
FROM adms.congestion_data
WHERE year = 2017
GROUP BY link_id
;


/*
Check 2017 failure rates for 2016 sensors with 100% failure rates
*/
SELECT exports.jk_annual_failure_rate_2017.link_id, exports.jk_annual_failure_rate_2017.failure_rate AS failure_rate_2017
FROM exports.jk_annual_failure_rate_2017
INNER JOIN (
    SELECT *
    FROM exports.jk_annual_failure_rate_2016 
    WHERE failure_rate = 1
) d ON exports.jk_annual_failure_rate_2017.link_id = d.link_id
;

-- Check
SELECT *
FROM exports.jk_annual_failure_rate_2016
WHERE link_id = '21315'
; -- output: 1

SELECT *
FROM exports.jk_annual_failure_rate_2017
WHERE link_id = '21315'
; -- output: 0.747...

-- number of sensors with failure rate < 1 in 2016, but with failure rate = 1 in 2017
SELECT count(*)
FROM (
    SELECT exports.jk_annual_failure_rate_2017.link_id, exports.jk_annual_failure_rate_2017.failure_rate AS failure_rate_2017
    FROM exports.jk_annual_failure_rate_2017
    INNER JOIN (
        SELECT *
        FROM exports.jk_annual_failure_rate_2016 
        WHERE failure_rate = 1
    ) d ON exports.jk_annual_failure_rate_2017.link_id = d.link_id
) d2
WHERE d2.failure_rate_2017 < 1
; -- only 14 -> i.e., most sensors failing in 2016 were also failing in 2017

/*
- Try query on a sample date: 8/15/2016
- Aggregate volume to each hour 
- Total rows = (count_sensors) x (hours_in_day) = 31,717 * 24 = 761,208 rows
*/
SELECT 
    d.link_id, 
    d.hour_local, 
    sum(d.volume) as volume
FROM (
    SELECT 
        link_id,
        day(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))   AS day_local,
        hour(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))  AS hour_local,
        volume
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 8
) d
WHERE d.day_local = 15
GROUP BY d.link_id, d.hour_local
ORDER BY d.link_id, d.hour_local;


SELECT 
    d.link_id, 
    sum(d.volume) as volume
FROM (
    SELECT 
        link_id,
        volume
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 8 AND day = 15 AND hour(from_utc_timestamp(date_and_time, 'America/Los_Angeles')) = 15 -- UTC = PST + 7
) d
GROUP BY d.link_id;


-- Now let's try aggregating by hour and seeing how many records we get
SELECT count(*)
FROM (
    SELECT 
        link_id,
        to_date(from_utc_timestamp(date_and_time, 'America/Los_Angeles')) AS date_string,
        hour(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))  AS hour_of_day
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 11 AND day = 1
    GROUP BY link_id, date_string, hour_of_day
) d;


-- SCRATCH 
SELECT count(*)
FROM (
    SELECT 
        link_id,
        to_date(from_utc_timestamp(date_and_time, 'America/Los_Angeles')) AS date_string,
        hour(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))  AS hour_of_day
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 11 AND day = 1
    GROUP BY link_id, date_string, hour_of_day
) d;


SELECT count(*)
FROM (
    SELECT  link_id,
            year(from_utc_timestamp(date_and_time, 'America/Los_Angeles')) AS year_local,
            month(from_utc_timestamp(date_and_time, 'America/Los_Angeles')) AS month_of_year,
            day(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))   AS day_of_month,
            hour(from_utc_timestamp(date_and_time, 'America/Los_Angeles'))  AS hour_of_day,
            sum(volume)                                                     AS total_volume
    FROM adms.congestion_data 
    WHERE year = 2015 OR year = 2016 OR year = 2017
    GROUP BY link_id, year_local, month_of_year, day_of_month, hour_of_day
) d
WHERE d.year_local = 2016;


SELECT  d.link_id,
        to_date(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles')) AS date_string,
        hour(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles'))  AS hour_of_day,
        month(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles')) AS month_of_year,
        day(from_utc_timestamp(d.date_and_time, 'America/Los_Angeles'))   AS day_of_month,
        sum(d.volume)                                                     AS total_volume,
        sum(d.success)                                                  AS count_success,
        sum(d.failure)                                                  AS count_failure,
        (sum(d.failure) / (sum(d.success)  + sum(d.failure))) * 100           AS failure_rate 
FROM (
    SELECT
        link_id,
        date_and_time,
        volume, 
        if(link_status = "OK", 1, 0)   AS success,
        if(link_status = "OK", 0, 1)   AS failure
    FROM adms.congestion_data
    WHERE year = 2016 AND month = 11 AND day = 1 AND link_id = '141388'
) d
GROUP BY link_id, date_string, month_of_year, day_of_month, hour_of_day
ORDER BY month_of_year, day_of_month, hour_of_day; 


