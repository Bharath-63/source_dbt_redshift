{{ config (
    materialized="table"
)}}
      
      select * from(

with __dbt__cte__LISTS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".PUBLIC._AIRBYTE_RAW_LISTS
select
    get_path(parse_json(_airbyte_data), '"golf"') as GOLF,
    get_path(parse_json(_airbyte_data), '"cricket"') as CRICKET,
    get_path(parse_json(_airbyte_data), '"football"') as FOOTBALL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    current_timestamp as _AIRBYTE_NORMALIZED_AT
from "DB".PUBLIC._AIRBYTE_RAW_LISTS as table_alias
-- LISTS
where 1 = 1

),  __dbt__cte__LISTS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__LISTS_AB1
select
    GOLF,
    CRICKET,
    FOOTBALL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    current_timestamp as _AIRBYTE_NORMALIZED_AT

from __dbt__cte__LISTS_AB1
-- LISTS
where 1 = 1

),  __dbt__cte__LISTS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__LISTS_AB2
select
    md5(cast(coalesce(cast(GOLF as
    varchar
), '') || '-' || coalesce(cast(CRICKET as
    varchar
), '') || '-' || coalesce(cast(FOOTBALL as
    varchar
), '') as
    varchar
)) as _AIRBYTE_LISTS_HASHID,
    tmp.*
from __dbt__cte__LISTS_AB2 tmp
-- LISTS
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__LISTS_AB3
select
    GOLF,
    CRICKET,
    FOOTBALL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
        current_timestamp as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_LISTS_HASHID
from __dbt__cte__LISTS_AB3
-- LISTS from "DB".PUBLIC._AIRBYTE_RAW_LISTS
where 1 = 1

            ) order by (_AIRBYTE_EMITTED_AT)
 
