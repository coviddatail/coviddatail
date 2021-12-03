#!/bin/sh
for x in $(bq ls --transfer_config --transfer_location=us --project_id=coviddatail | cut -d"/" -f6 | tail +3 | cut -d" " -f1)
bq rm  -f --location us --transfer_config projects/coviddatail/locations/us/transferConfigs/$xbq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=lab2020 \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/lab2020/*/lab2020_*.csv", "destination_table_name_template": "lab2020", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=lab2021b \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/lab2021b/*/lab2021b_*.csv", "destination_table_name_template": "lab2021b", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=tested_individuals_features \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/tested_individuals_features/*/tested_individuals_features_*.csv", "destination_table_name_template": "tested_individuals_features", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=tested_individuals_features_last2w \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/tested_individuals_features_last2w/*/tested_individuals_features_last2w_*.csv", "destination_table_name_template": "tested_individuals_features_last2w", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=tested_individuals_features_helper \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/tested_individuals_features_helper/*/tested_individuals_features_helper_*.csv", "destination_table_name_template": "tested_individuals_features_helper", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=covid19_by_area \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/covid19_by_area/*/covid19_by_area_*.csv", "destination_table_name_template": "covid19_by_area", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=covid19_by_age \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/covid19_by_age/*/covid19_by_age_*.csv", "destination_table_name_template": "covid19_by_age", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=covid19_infection_hospital_crew \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/covid19_infection_hospital_crew/*/covid19_infection_hospital_crew_*.csv", "destination_table_name_template": "covid19_infection_hospital_crew", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=covid19_deaths \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/covid19_deaths/*/covid19_deaths_*.csv", "destination_table_name_template": "covid19_deaths", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=covid19_communities \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/covid19_communities/*/covid19_communities_*.csv", "destination_table_name_template": "covid19_communities", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=vaccinated_age_groups \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/vaccinated_age_groups/*/vaccinated_age_groups_*.csv", "destination_table_name_template": "vaccinated_age_groups", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=deaths_related_to_vaccine \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/deaths_related_to_vaccine/*/deaths_related_to_vaccine_*.csv", "destination_table_name_template": "deaths_related_to_vaccine", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=vaccinated_communities \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/vaccinated_communities/*/vaccinated_communities_*.csv", "destination_table_name_template": "vaccinated_communities", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=vaccinated_groups \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/vaccinated_groups/*/vaccinated_groups_*.csv", "destination_table_name_template": "vaccinated_groups", "max_bad_records": "0"}'
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=positives_after_vaccination \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/datagov/positives_after_vaccination/*/positives_after_vaccination_*.csv", "destination_table_name_template": "positives_after_vaccination", "max_bad_records": "0"}'


# CBS
    

bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=cbs_deaths_1 \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/cbs/deaths_report_1/*/*.csv", "destination_table_name_template": "deaths_report_1", "max_bad_records": "0"}'


bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=cbs_deaths_2 \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/cbs/deaths_report_2/*/*.csv", "destination_table_name_template": "deaths_report_2", "max_bad_records": "0"}'


bq mk     --transfer_config \
    --project_id=coviddatail \
    --data_source=google_cloud_storage \
    --display_name=cbs_deaths_3 \
    --target_dataset=staging \
    --params='{"field_delimiter": ",", "skip_leading_rows": "1", "write_disposition": "APPEND", "file_format": "CSV", "data_path_template": "gs://coviddatail/cbs/deaths_report_1/*/*.csv", "destination_table_name_template": "deaths_report_3", "max_bad_records": "0"}'


