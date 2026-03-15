{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Stroke Analysis (Infosys API).
    One row per point from the rallyShots.allPoints array.
    Each row contains point-level details (shot type, hand, outcome, etc.)
    along with match metadata and player information.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_stroke_analysis') }}

),

base as (

    select
        json_extract(data, '$.data')  as payload,
        meta_file_name,
        meta_file_modified
    from raw

),

points as (

    select
        b.payload,
        b.meta_file_name,
        b.meta_file_modified,
        unnest(
            cast(json_extract(b.payload, '$.rallyShots.allPoints') as json[])
        ) as pt
    from base as b

),

parsed as (

    select
        -- Match identification (parsed from S3 path)
        cast(regexp_extract(meta_file_name, 'year=(\d+)', 1) as int)          as year,
        regexp_extract(meta_file_name, 'tourn=([^/]+)', 1)                    as tournament_id,
        regexp_extract(meta_file_name, '/([^/_]+)_\d{8}_\d{6}\.json', 1)      as match_id,

        -- Match metadata
        cast(json_extract(payload, '$.setsCompleted') as int)                 as sets_completed,
        json_extract_string(payload, '$.matchCompleted')                      as match_completed,
        json_extract_string(payload, '$.isDoubles')                           as is_doubles,
        json_extract_string(payload, '$.courtId')                             as court_id,

        -- Player 1 info
        json_extract_string(payload, '$.players[0].seed')                     as player1_seed,
        json_extract_string(payload, '$.players[0].player1Name')              as player1_name,
        json_extract_string(payload, '$.players[0].player1Id')                as player1_id,
        json_extract_string(payload, '$.players[0].player1Country')           as player1_country,

        -- Player 2 info
        json_extract_string(payload, '$.players[1].seed')                     as player2_seed,
        json_extract_string(payload, '$.players[1].player1Name')              as player2_name,
        json_extract_string(payload, '$.players[1].player1Id')                as player2_id,
        json_extract_string(payload, '$.players[1].player1Country')           as player2_country,

        -- Point-level fields
        json_extract_string(pt, '$.pointId')                                  as point_id,
        cast(json_extract(pt, '$.set') as int)                                as set_number,
        json_extract_string(pt, '$.score')                                    as score,
        cast(json_extract(pt, '$.serve') as int)                              as serve,
        json_extract_string(pt, '$.serveDir')                                 as serve_dir,
        json_extract_string(pt, '$.courtSide')                                as court_side,
        cast(json_extract(pt, '$.serveSpeed') as double)                      as serve_speed,
        json_extract_string(pt, '$.hand')                                     as hand,
        json_extract_string(pt, '$.shotType')                                 as shot_type,
        json_extract_string(pt, '$.pointEndType')                             as point_end_type,
        json_extract_string(pt, '$.rallyLength')                              as rally_length,
        json_extract_string(pt, '$.crucialPoint')                             as crucial_point,
        json_extract_string(pt, '$.t1BreakPoint')                             as t1_break_point,
        json_extract_string(pt, '$.t2BreakPoint')                             as t2_break_point,
        json_extract_string(pt, '$.t1NetPoint')                               as t1_net_point,
        json_extract_string(pt, '$.t2NetPoint')                               as t2_net_point,
        json_extract_string(pt, '$.tieBreak')                                 as tie_break,
        json_extract_string(pt, '$.setPoint')                                 as set_point,

        -- File metadata
        meta_file_name,
        meta_file_modified

    from points

)

select * from parsed
