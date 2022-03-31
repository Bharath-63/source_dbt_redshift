{{ config (
    materialized="table"
)}}

with "covid_data_tmp"
as
(select
            _airbyte_emitted_at,

            cast(json_extract_path_text("_airbyte_data",'key') as varchar) as id,
            cast(json_extract_path_text("_airbyte_data",'date') as varchar) as updated_at,
            cast(json_extract_path_text("_airbyte_data",'new_tested') as float) as new_tested,
            cast(json_extract_path_text("_airbyte_data",'new_deceased') as float) as new_deceased,
            cast(json_extract_path_text("_airbyte_data",'total_tested') as float) as total_tested,
            cast(json_extract_path_text("_airbyte_data",'new_confirmed') as float) as new_confirmed,
            cast(json_extract_path_text("_airbyte_data",'new_recovered') as float) as new_recovered,
            cast(json_extract_path_text("_airbyte_data",'total_deceased') as float) as total_deceased,
            cast(json_extract_path_text("_airbyte_data",'total_confirmed') as float) as total_confirmed,
            cast(json_extract_path_text("_airbyte_data",'total_recovered') as float) as total_recovered
        from "dev".public._airbyte_raw_covid_epidemiology as table_alias
 )
 
 select * from "covid_data_tmp"
