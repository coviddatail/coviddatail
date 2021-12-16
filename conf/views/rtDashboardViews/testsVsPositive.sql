SELECT date,amount,positiveAmount,mov_avg_7d_tests,mov_avg_7d_positive FROM (
SELECT date, amount,positiveAmount,
avg(amount) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_tests,
avg(positiveAmount) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_positive,
FROM (
    SELECT date,UNIX_DATE(DATE(date)) AS day, SAFE_CAST(amount as numeric) as amount, SAFE_CAST(positiveAmount as numeric) as positiveAmount
    FROM `coviddatail.staging.testResultsPerDate`
    WHERE batch_date=(SELECT max(batch_date) FROM `coviddatail.staging.testResultsPerDate`)
    ORDER BY date ASC
)
)