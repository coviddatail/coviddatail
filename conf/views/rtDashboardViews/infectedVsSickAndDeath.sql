SELECT date,infected, new_hospitalized, serious_critical_new,deaths,
avg(infected) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_infected,
avg(new_hospitalized) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_new_hospitalized,
avg(serious_critical_new) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_serious_critical_new,
avg(deaths) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_deaths
FROM (
    SELECT i.date,UNIX_DATE(DATE(i.date)) AS day,
    SAFE_CAST(i.amount AS NUMERIC) AS infected,
    SAFE_CAST(p.new_hospitalized AS NUMERIC) AS new_hospitalized,
    SAFE_CAST(p.serious_critical_new AS NUMERIC) AS serious_critical_new,
    SAFE_CAST(d.amount AS NUMERIC) AS deaths
    FROM `coviddatail.staging.infectedPerDate` i 
    JOIN `coviddatail.staging.patientsPerDate` p ON DATE_ADD(DATE(i.date) , INTERVAL 7 DAY)=DATE(p.date) AND i.batch_date=p.batch_date
    JOIN `coviddatail.staging.deadPatientsPerDate` d ON DATE_ADD(DATE(i.date) , INTERVAL 14 DAY)=DATE(d.date) AND i.batch_date=d.batch_date
    WHERE i.batch_date=(SELECT MAX(batch_date) FROM `coviddatail.staging.infectedPerDate`)
    ORDER BY i.date ASC
)
ORDER BY date ASC