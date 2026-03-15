{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Rally Analysis (Infosys API).
    One row per point, derived from rallyData shot groups.
    Each shot group (Serve, Return, 3rd shot, ...) contains four sub-arrays:
      t1win, t1err, t2win, t2err
    We unnest all four and tag each point with the shot_group name and
    outcome_type (t1_win, t1_error, t2_win, t2_error).
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_rally_analysis') }}

),

base as (

    select
        json_extract(data, '$.data')  as payload,
        meta_file_name,
        meta_file_modified
    from raw

),

{#
    Unnest the rallyData array into one row per shot group,
    then cross join with the four outcome types.
#}

shot_groups as (

    select
        b.payload,
        b.meta_file_name,
        b.meta_file_modified,
        unnest(cast(json_extract(b.payload, '$.rallyData') as json[])) as sg
    from base as b

),

outcome_types as (

    select * from (
        values
            ('t1win',  't1_win'),
            ('t1err',  't1_error'),
            ('t2win',  't2_win'),
            ('t2err',  't2_error')
    ) as t(json_key, outcome_type)

),

points as (

    select
        sg_out.payload,
        sg_out.meta_file_name,
        sg_out.meta_file_modified,
        json_extract_string(sg_out.sg, '$.name')  as shot_group,
        ot.outcome_type,
        unnest(
            cast(json_extract(sg_out.sg, '$.' || ot.json_key) as json[])
        ) as pt
    from shot_groups as sg_out
    cross join outcome_types as ot
    where json_array_length(json_extract(sg_out.sg, '$.' || ot.json_key)) > 0

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

        -- Player 1 info
        json_extract_string(payload, '$.playerDetails[0].seed')               as player1_seed,
        json_extract_string(payload, '$.playerDetails[0].player1Name')        as player1_name,
        json_extract_string(payload, '$.playerDetails[0].player1Id')          as player1_id,
        json_extract_string(payload, '$.playerDetails[0].player1Country')     as player1_country,

        -- Player 2 info
        json_extract_string(payload, '$.playerDetails[1].seed')               as player2_seed,
        json_extract_string(payload, '$.playerDetails[1].player1Name')        as player2_name,
        json_extract_string(payload, '$.playerDetails[1].player1Id')          as player2_id,
        json_extract_string(payload, '$.playerDetails[1].player1Country')     as player2_country,

        -- Rally / shot group context
        shot_group,
        outcome_type,

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
