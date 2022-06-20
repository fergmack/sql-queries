----- ***** ///// TEST GROUPS \\\\\ ***** -----
----- ***** /////--------------\\\\ ***** -----

-- // Step 1: Calculate AB results
CREATE OR REPLACE TABLE ab.results_ab
AS
SELECT MIN(date) test_start_date, 
MAX(date) test_end_date, 
experimentBucket, 
CAST(COUNT(*) AS INT64) AS number_of_users,
SUM(switched) AS switched, 
ROUND(SUM(switched) / COUNT(*), 4) AS conversion_rate
FROM `ab-testing-274018.ab.raw_ab` 
WHERE experimentBucket NOT IN ('None')
GROUP BY 3;

-- // Step 2: Device
CREATE OR REPLACE TABLE ab.results_ab_device
AS
SELECT MIN(date) test_start_date, 
MAX(date) test_end_date, 
experimentBucket, 
deviceType,
CAST(COUNT(*) AS INT64) AS number_of_users,
COUNTIF( switched = 1) switched, 
ROUND(countif(switched = 1) / COUNT(*), 4) AS conversion_rate
FROM `ab-testing-274018.ab.raw_ab` 
WHERE experimentBucket NOT IN ('None')
GROUP BY 3, 4;

-- // Step 3: Channel
CREATE OR REPLACE TABLE ab.results_ab_channel
AS
SELECT MIN(date) test_start_date, 
MAX(date) test_end_date, 
experimentBucket,
channel, 
CAST(COUNT(*) AS INT64) AS number_of_users,
COUNTIF( switched = 1) switched, 
ROUND(countif(switched = 1) / COUNT(*), 4) AS conversion_rate
FROM `ab-testing-274018.ab.raw_ab` 
WHERE experimentBucket NOT IN ('None')
GROUP BY 3, 4;

----- ***** ///// NON-TEST GROUPS \\\\\ ***** -----
----- ***** /////--------------\\\\ ***** -----
CREATE OR REPLACE TABLE ab.non_test_metrics
AS
SELECT CASE WHEN day = '1' THEN 'Sunday' 
WHEN day = '2' THEN 'Monday'
WHEN day = '3' THEN 'Tueday'
WHEN day = '4' THEN 'Wednesday'
WHEN day = '5' THEN 'Thurday'
WHEN day = '6' THEN 'Friday'
WHEN day = '7' THEN 'Saturday'
END week_day,
*
FROM
(
        SELECT date,
        CAST(EXTRACT(DAYOFWEEK FROM date) AS STRING) as day,
        EXTRACT(DAYOFWEEK FROM DATE) as day_int,
        deviceType,
        channel,
        maximumSavingsSeen,
        switched
        FROM `ab-testing-274018.ab.raw_ab` 
        WHERE experimentBucket in ('None')   
        );
        
       ---------
        CREATE OR REPLACE TABLE ab.non_test_metrics_timeSeries
        AS
        SELECT CONCAT( (CASE WHEN month = '2' THEN 'February'
                    When month = '3' THEN 'March' END), '-', week_day) AS month_day, 
                    *
                    FROM 
       ( select CAST(EXTRACT(MONTH FROM date) AS STRING) as month, *     
        from ab.non_test_metrics )
       ; 
-- averages
CREATE OR REPLACE TABLE ab.nontest_day_averages
AS
select week_day, Round(sum(switched)/count(date)) average_switched
from 
(
select  date, week_day, sum(switched) switched from ab.non_test_metrics
where switched = 1
group by 1
, 2
)
group by 1
;
-- 7-day rolling average
CREATE OR REPLACE TABLE ab.mv_avg_7day
AS
SELECT idx, date,
sum(switched) switched_totals, ROUND(AVG(switched) OVER (ORDER BY idx RANGE BETWEEN 7 PRECEDING AND CURRENT ROW)) AS moving_average_7day
FROM (
SELECT 
ROW_NUMBER() OVER (ORDER BY DATE) AS idx,
date, 
sum(switched) switched
FROM ab.non_test_metrics
GROUP BY date
)
GROUP BY 1, 2, switched
;
-- what price causes them to change 
CREATE OR REPLACE TABLE ab.amount_to_switch
AS
SELECT CASE WHEN switched = 1 THEN 'Switched' ELSE 'Did not Switch' END action,
-- sum(switched) switched, 
ROUND(AVG(maximumSavingsSeen))  average_savings_seen
FROM ab.non_test_metrics 
GROUP by 1
;
-- Targets
CREATE OR REPLACE TABLE ab.non_switch_targets
AS
SELECT date, visitID, CASE WHEN switched = 0 THEN 'Did not switch' END switched, maximumSavingsSeen
FROM ab.raw_ab
WHERE switched = 0
AND experimentBucket  IN ('None')
ORDER BY maximumSavingsSeen DESC


-- GROUP BY 1, 2, 3, 4
/*
-- channel 
with test as (
select 
channel, 
-- deviceType, 
switched
-- avg(maximumSavingsSeen) average_savings_seen
FROM `ab-testing-274018.ab.raw_ab` 
where experimentBucket in ('None')
) select count(*) users, channel,  countif(switched = 1), sum(switched)
from test
group by 2
*/
