SELECT date,deaths,vaccinated,mov_avg_7d_deaths,mov_avg_7d_vaccinated FROM (
SELECT date, deaths,vaccinated,
avg(deaths) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_deaths,
avg(vaccinated) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_vaccinated,
FROM (
    SELECT res.date,UNIX_DATE(DATE(res.date)) AS day, avg(SAFE_CAST(amount as NUMERIC)) as deaths, sum(SAFE_CAST(first_dose_0_19 AS NUMERIC)+SAFE_CAST(first_dose_20_29 AS NUMERIC)+SAFE_CAST(first_dose_30_39 AS NUMERIC)+SAFE_CAST(first_dose_40_49 AS NUMERIC)+SAFE_CAST(first_dose_50_59 AS NUMERIC)+SAFE_CAST(first_dose_60_69 AS NUMERIC)+SAFE_CAST(first_dose_70_79 AS NUMERIC)+SAFE_CAST(first_dose_80_89 AS NUMERIC)+SAFE_CAST(first_dose_90_ AS NUMERIC)+
        SAFE_CAST(second_dose_0_19 AS NUMERIC)+SAFE_CAST(second_dose_20_29 AS NUMERIC)+SAFE_CAST(second_dose_30_39 AS NUMERIC)+SAFE_CAST(second_dose_40_49 AS NUMERIC)+SAFE_CAST(second_dose_50_59 AS NUMERIC)+SAFE_CAST(second_dose_60_69 AS NUMERIC)+SAFE_CAST(second_dose_70_79 AS NUMERIC)+SAFE_CAST(second_dose_80_89 AS NUMERIC)+SAFE_CAST(second_dose_90_ AS NUMERIC)+
        SAFE_CAST(third_dose_0_19 AS NUMERIC)+SAFE_CAST(third_dose_20_29 AS NUMERIC)+SAFE_CAST(third_dose_30_39 AS NUMERIC)+SAFE_CAST(third_dose_40_49 AS NUMERIC)+SAFE_CAST(third_dose_50_59 AS NUMERIC)+SAFE_CAST(third_dose_60_69 AS NUMERIC)+SAFE_CAST(third_dose_70_79 AS NUMERIC)+SAFE_CAST(third_dose_80_89 AS NUMERIC)+SAFE_CAST(third_dose_90_ AS NUMERIC)) 
        as vaccinated
    FROM `coviddatail.staging.deadPatientsPerDate` res JOIN `coviddatail.staging.vaccinated_communities` vax ON UNIX_DATE(DATE(res.date))=UNIX_DATE(DATE(vax.Date))
    WHERE res.batch_date=(SELECT max(batch_date) FROM `coviddatail.staging.deadPatientsPerDate`)
    GROUP BY res.date
    ORDER BY res.date ASC
)
)
ORDER BY date ASC