{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Tournament Results.
    Flattens the JSON match data and deduplicates so only the most recent
    instance of each (year, tournament_id, match_id) combination is shown.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_tournament_results') }}

),

flattened as (

    select
        cast(json_extract_string(t.j, '$.year') as int)       as year,
        json_extract_string(t.j, '$.tournament_name')          as tournament_name,
        json_extract_string(t.j, '$.tournament_id')            as tournament_id,
        json_extract_string(t.j, '$.round')                    as round,
        json_extract_string(t.j, '$.match_id')                 as match_id,
        json_extract_string(t.j, '$.player1_name')             as player1_name,
        json_extract_string(t.j, '$.player1_id')               as player1_id,
        json_extract_string(t.j, '$.player1_seed')             as player1_seed,
        json_extract_string(t.j, '$.player1_nation')           as player1_nation,
        json_extract_string(t.j, '$.player2_name')             as player2_name,
        json_extract_string(t.j, '$.player2_id')               as player2_id,
        json_extract_string(t.j, '$.player2_seed')             as player2_seed,
        json_extract_string(t.j, '$.player2_nation')           as player2_nation,
        json_extract_string(t.j, '$.score')                    as score,
        json_extract_string(t.j, '$.url')                      as url,
        raw.meta_file_name,
        raw.meta_file_modified
    from raw,
         lateral unnest(
             cast(json_extract(raw.data, '$.data') as json[])
         ) as t(j)

),

deduped as (

    select *
    from flattened
    qualify row_number() over (
        partition by year, tournament_id, match_id
        order by meta_file_modified desc
    ) = 1

)

select * from deduped
