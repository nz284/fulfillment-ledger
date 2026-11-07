-- psql -v tenant=RUN_ID -v seconds=120 -f scripts/capacity-report.sql
-- Anchor every bucket to the first admission for this run; include empty seconds.
\set ON_ERROR_STOP on
CREATE TEMP TABLE capacity_cohort AS
SELECT o.created_at AS admitted, h.created_at AS confirmed
FROM fulfillment_order o
LEFT JOIN history h ON h.order_id=o.id AND h.state='CONFIRMED'
WHERE o.tenant=:'tenant';

SELECT count(*) AS orders, count(confirmed) AS confirmations,
       round(count(confirmed)/nullif(extract(epoch FROM max(confirmed)-min(admitted)),0),2) AS confirmed_per_s,
       round(extract(epoch FROM max(confirmed)-max(admitted)),3) AS drain_seconds,
       percentile_cont(.99) WITHIN GROUP (ORDER BY extract(epoch FROM confirmed-admitted)) AS p99_seconds
FROM capacity_cohort;

WITH origin AS (SELECT min(admitted) AS start FROM capacity_cohort),
buckets AS (SELECT generate_series(0,(:seconds)::int-1) AS bucket),
arrivals AS (
    SELECT floor(extract(epoch FROM admitted-start))::int AS bucket, count(*) AS n
    FROM capacity_cohort,origin GROUP BY 1
), completions AS (
    SELECT floor(extract(epoch FROM confirmed-start))::int AS bucket, count(*) AS n
    FROM capacity_cohort,origin WHERE confirmed IS NOT NULL GROUP BY 1
)
SELECT bucket AS second, coalesce(arrivals.n,0) AS admitted, coalesce(completions.n,0) AS confirmed
FROM buckets LEFT JOIN arrivals USING(bucket) LEFT JOIN completions USING(bucket)
ORDER BY bucket;
