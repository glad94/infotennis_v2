{{
    config(
        materialized='view'
    )
}}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_calendar_test') }}

),

flattened as (

    select
        cast(json_extract_string(t.j, '$.year') as int)    as year,
        json_extract_string(t.j, '$.tournament')            as tournament,
        json_extract_string(t.j, '$.tournament_id')         as tournament_id,
        json_extract_string(t.j, '$.category')              as category,
        json_extract_string(t.j, '$.city')                  as city,
        json_extract_string(t.j, '$.country')               as country,
        json_extract_string(t.j, '$.dates')                 as dates,
        json_extract_string(t.j, '$.singles_winner')        as singles_winner,
        case
            when json_extract(t.j, '$.doubles_winner') is null
                 or json_type(json_extract(t.j, '$.doubles_winner')) = 'NULL'
                then null
            when json_type(json_extract(t.j, '$.doubles_winner')) = 'ARRAY'
                then array_to_string(
                    cast(json_extract(t.j, '$.doubles_winner') as varchar[]),
                    ', '
                )
            else json_extract_string(t.j, '$.doubles_winner')
        end                                                 as doubles_winner,
        json_extract_string(t.j, '$.url')                   as url,
        raw.meta_file_name,
        raw.meta_file_modified
    from raw,
         lateral unnest(
             cast(json_extract(raw.data, '$.data') as json[])
         ) as t(j)

)

select * from flattened
