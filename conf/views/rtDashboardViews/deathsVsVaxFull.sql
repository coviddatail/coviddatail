with grouped as
(select 
    DATE(Date)  report_date, 
    sum(SAFE_CAST( (CASE WHEN first_dose_0_19='<15' THEN '15' ELSE first_dose_0_19 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_20_29='<15' THEN '15' ELSE first_dose_20_29 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_30_39='<15' THEN '15' ELSE first_dose_30_39 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_40_49='<15' THEN '15' ELSE first_dose_40_49 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_50_59='<15' THEN '15' ELSE first_dose_50_59 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_60_69='<15' THEN '15' ELSE first_dose_60_69 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_70_79='<15' THEN '15' ELSE first_dose_70_79 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_80_89='<15' THEN '15' ELSE first_dose_80_89 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN first_dose_90_='<15' THEN '15' ELSE first_dose_90_ END) AS NUMERIC))  first_dose_cummulative,
    sum(SAFE_CAST( (CASE WHEN second_dose_0_19='<15' THEN '15' ELSE second_dose_0_19 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_20_29='<15' THEN '15' ELSE second_dose_20_29 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_30_39='<15' THEN '15' ELSE second_dose_30_39 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_40_49='<15' THEN '15' ELSE second_dose_40_49 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_50_59='<15' THEN '15' ELSE second_dose_50_59 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_60_69='<15' THEN '15' ELSE second_dose_60_69 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_70_79='<15' THEN '15' ELSE second_dose_70_79 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_80_89='<15' THEN '15' ELSE second_dose_80_89 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN second_dose_90_='<15' THEN '15' ELSE second_dose_90_ END) AS NUMERIC))  second_dose_cummulative,
    sum(SAFE_CAST( (CASE WHEN third_dose_0_19='<15' THEN '15' ELSE third_dose_0_19 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_20_29='<15' THEN '15' ELSE third_dose_20_29 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_30_39='<15' THEN '15' ELSE third_dose_30_39 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_40_49='<15' THEN '15' ELSE third_dose_40_49 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_50_59='<15' THEN '15' ELSE third_dose_50_59 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_60_69='<15' THEN '15' ELSE third_dose_60_69 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_70_79='<15' THEN '15' ELSE third_dose_70_79 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_80_89='<15' THEN '15' ELSE third_dose_80_89 END) AS NUMERIC)+
SAFE_CAST( (CASE WHEN third_dose_90_='<15' THEN '15' ELSE third_dose_90_ END) AS NUMERIC))  third_dose_cummulative,
    count(*) record_count
from
`coviddatail.staging.vaccinated_communities`

group by report_date
order by report_date ),
/* SELECT only records that have all cities loaded */

only_full as (
select * from grouped where record_count>10
),
diffed as (
select report_date,
O.first_dose_cummulative,second_dose_cummulative,third_dose_cummulative,
SUM(first_dose_cummulative)  OVER (ORDER BY report_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS first_dose_cummulative_yesterday,
SUM(second_dose_cummulative)  OVER (ORDER BY report_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS second_dose_cummulative_yesterday,
SUM(third_dose_cummulative)  OVER (ORDER BY report_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS third_dose_cummulative_yesterday

from only_full O
)


SELECT date,deaths, new_vaccinated ,
avg(deaths) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_deaths,
avg(new_vaccinated) OVER (ORDER BY day RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS mov_avg_7d_new_vaccinated
FROM (
select d.date,UNIX_DATE(DATE(d.date)) AS day, first_dose_cummulative - first_dose_cummulative_yesterday first_dose,
          second_dose_cummulative - second_dose_cummulative_yesterday second_dose,
          third_dose_cummulative - third_dose_cummulative_yesterday third_dose,
          GREATEST(0,first_dose_cummulative - first_dose_cummulative_yesterday +  second_dose_cummulative - second_dose_cummulative_yesterday + third_dose_cummulative - third_dose_cummulative_yesterday) new_vaccinated,
          SAFE_CAST(amount as NUMERIC) as deaths
          from diffed vax JOIN `coviddatail.staging.deadPatientsPerDate` d ON UNIX_DATE(DATE(d.date))=UNIX_DATE(DATE(vax.report_date))
          WHERE d.batch_date=(SELECT max(batch_date) FROM `coviddatail.staging.deadPatientsPerDate`)
)