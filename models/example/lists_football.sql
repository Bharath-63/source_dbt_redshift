{{ config (
    materialized="table"
)}}
select * from(

with __dbt__cte__lists_football_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "dev".public."lists"
with numbers as (




    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select


    p0.generated_number * power(2, 0)
     +

    p1.generated_number * power(2, 1)
     +

    p2.generated_number * power(2, 2)
     +

    p3.generated_number * power(2, 3)


    + 1
    as generated_number

    from


    p as p0
     cross join

    p as p1
     cross join

    p as p2
     cross join

    p as p3



    )

    select *
    from unioned
    where generated_number <= 10
    order by generated_number


),
joined as (
    select
        _airbyte_lists_hashid as _airbyte_hashid,
        json_extract_array_element_text(football, numbers.generated_number::int - 1, true) as _airbyte_nested_data
    from "dev".public."lists"
    cross join numbers
    -- only generate the number of records in the cross join that corresponds
    -- to the number of items in "dev".public."lists".football
    where numbers.generated_number <= json_array_length(football, true)
)
select
    _airbyte_lists_hashid,
    case when json_extract_path_text(_airbyte_nested_data, 'match', true) != '' then json_extract_path_text(_airbyte_nested_data, 'match', true) end as match,
    case when json_extract_path_text(_airbyte_nested_data, 'start', true) != '' then json_extract_path_text(_airbyte_nested_data, 'start', true) end as start,
    case when json_extract_path_text(_airbyte_nested_data, 'region', true) != '' then json_extract_path_text(_airbyte_nested_data, 'region', true) end as region,
    case when json_extract_path_text(_airbyte_nested_data, 'country', true) != '' then json_extract_path_text(_airbyte_nested_data, 'country', true) end as country,
    case when json_extract_path_text(_airbyte_nested_data, 'stadium', true) != '' then json_extract_path_text(_airbyte_nested_data, 'stadium', true) end as stadium,
    case when json_extract_path_text(_airbyte_nested_data, 'tournament', true) != '' then json_extract_path_text(_airbyte_nested_data, 'tournament', true) end as tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "dev".public."lists" as table_alias
-- football at lists/football
left join joined on _airbyte_lists_hashid = joined._airbyte_hashid
where 1 = 1
and football is not null

),  __dbt__cte__lists_football_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__lists_football_ab1
select
    _airbyte_lists_hashid,
    cast(match as varchar) as match,
    cast(start as varchar) as start,
    cast(region as varchar) as region,
    cast(country as varchar) as country,
    cast(stadium as varchar) as stadium,
    cast(tournament as varchar) as tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__lists_football_ab1
-- football at lists/football
where 1 = 1

),  __dbt__cte__lists_football_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__lists_football_ab2
select
    md5(cast(coalesce(cast(_airbyte_lists_hashid as varchar), '') || '-' || coalesce(cast(match as varchar), '') || '-' || coalesce(cast(start as varchar), '') || '-' || coalesce(cast(region as varchar), '') || '-' || coalesce(cast(country as varchar), '') || '-' || coalesce(cast(stadium as varchar), '') || '-' || coalesce(cast(tournament as varchar), '') as varchar)) as _airbyte_football_hashid,
    tmp.*
from __dbt__cte__lists_football_ab2 tmp
-- football at lists/football
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__lists_football_ab3
select
    _airbyte_lists_hashid,
    match,
    start,
    region,
    country,
    stadium,
    tournament,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at,
    _airbyte_football_hashid
from __dbt__cte__lists_football_ab3
-- football at lists/football from "dev".public."lists"
where 1 = 1

  );
