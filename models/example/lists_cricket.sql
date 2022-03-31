{{ config (
    materialized="table"
)}}

select * from(

with __dbt__cte__LISTS_CRICKET_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".PUBLIC."LISTS"

select
    _AIRBYTE_LISTS_HASHID,
    to_varchar(get_path(parse_json(CRICKET.value), '"match"')) as MATCH,
    to_varchar(get_path(parse_json(CRICKET.value), '"start"')) as "start",
    to_varchar(get_path(parse_json(CRICKET.value), '"region"')) as REGION,
    to_varchar(get_path(parse_json(CRICKET.value), '"country"')) as COUNTRY,
    to_varchar(get_path(parse_json(CRICKET.value), '"stadium"')) as STADIUM,
    to_varchar(get_path(parse_json(CRICKET.value), '"tournament"')) as TOURNAMENT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    current_timestamp as _AIRBYTE_NORMALIZED_AT
from "DB".PUBLIC."LISTS" as table_alias
-- CRICKET at lists/cricket
cross join table(flatten(CRICKET)) as CRICKET
where 1 = 1
and CRICKET is not null

),  __dbt__cte__LISTS_CRICKET_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__LISTS_CRICKET_AB1
select
    _AIRBYTE_LISTS_HASHID,
    cast(MATCH as
    varchar
) as MATCH,
    cast("start" as
    varchar
) as "start",
    cast(REGION as
    varchar
) as REGION,
    cast(COUNTRY as
    varchar
) as COUNTRY,
    cast(STADIUM as
    varchar
) as STADIUM,
    cast(TOURNAMENT as
    varchar
) as TOURNAMENT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    current_timestamp as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__LISTS_CRICKET_AB1
-- CRICKET at lists/cricket
where 1 = 1

),  __dbt__cte__LISTS_CRICKET_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__LISTS_CRICKET_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_LISTS_HASHID as
    varchar
), '') || '-' || coalesce(cast(MATCH as
    varchar
), '') || '-' || coalesce(cast("start" as
    varchar
), '') || '-' || coalesce(cast(REGION as
    varchar
), '') || '-' || coalesce(cast(COUNTRY as
    varchar
), '') || '-' || coalesce(cast(STADIUM as
    varchar
), '') || '-' || coalesce(cast(TOURNAMENT as
    varchar
), '') as
    varchar
)) as _AIRBYTE_CRICKET_HASHID,
    tmp.*
from __dbt__cte__LISTS_CRICKET_AB2 tmp
-- CRICKET at lists/cricket
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__LISTS_CRICKET_AB3
select
    _AIRBYTE_LISTS_HASHID,
    MATCH,
    "start",
    REGION,
    COUNTRY,
    STADIUM,
    TOURNAMENT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    current_timestamp as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_CRICKET_HASHID
from __dbt__cte__LISTS_CRICKET_AB3
-- CRICKET at lists/cricket from "DB".PUBLIC."LISTS"
where 1 = 1

            ) order by (_AIRBYTE_EMITTED_AT)
