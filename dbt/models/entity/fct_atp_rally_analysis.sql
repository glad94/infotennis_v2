{#
    Entity fact table for ATP Rally Analysis.
    Translates the Python processing_rallys.py logic into SQL.

    Transformations:
      - Maps outcome_type (t1_win/t1_error/t2_win/t2_error) → outcome (W/L)
        and assigns player_id / opponent_id accordingly.
      - Corrects DOUBLE FAULT rows mis-categorised as wins (swaps player/opp, flips to L).
      - Derives shot_number from shot_group name (Serve→1, Return→2, …, 9+ odd, 10+ even).
      - Extracts game and point from point_id (format: set_game_point).
      - Deduplicates to the latest file version per match point.
#}

{{
    config(
        materialized='incremental',
        unique_key=['year', 'tournament_id', 'match_id', 'point_id']
    )
}}

with staging as (

    select * from {{ ref('stg_atp_rally_analysis') }}
    {% if is_incremental() %}
    where meta_file_modified > (select max(meta_file_modified) from {{ this }})
    {% endif %}

),

{#
    Step 1: Derive outcome (W/L), player_id, opponent_id, shot_number,
    and extract game/point from point_id.
#}

enriched as (

    select
        -- Match identification
        year,
        tournament_id,
        match_id,
        sets_completed,

        -- Shot number derived from shot_group name (mirrors Python numbering)
        case
            when shot_group = 'Serve'       then '1'
            when shot_group = 'Return'      then '2'
            when shot_group = '3rd shot'    then '3'
            when shot_group = '4th shot'    then '4'
            when shot_group = '5th shot'    then '5'
            when shot_group = '6th shot'    then '6'
            when shot_group = '7th shot'    then '7'
            when shot_group = '8th shot'    then '8'
            when lower(shot_group) like '9%'  then '9+_odd'
            when lower(shot_group) like '10%' then '10+_even'
            else 'Unknown'
        end as shot_number,

        -- Outcome: W for wins, L for errors
        case
            when outcome_type in ('t1_win', 't2_win')     then 'W'
            when outcome_type in ('t1_error', 't2_error') then 'L'
        end as outcome,

        -- Player ID: t1 outcomes → player1, t2 outcomes → player2
        case
            when outcome_type like 't1%' then player1_id
            else player2_id
        end as player_id,

        -- Opponent ID (inverse of player)
        case
            when outcome_type like 't1%' then player2_id
            else player1_id
        end as opponent_id,

        -- Point context
        point_id,
        set_number,
        split_part(point_id, '_', 2) as game,
        split_part(point_id, '_', 3) as point,
        score,
        serve,
        serve_dir,
        court_side,
        serve_speed,
        hand,
        shot_type,
        point_end_type,
        crucial_point,
        t1_break_point,
        t2_break_point,
        t1_net_point,
        t2_net_point,
        tie_break,
        set_point,

        -- File metadata
        meta_file_name,
        meta_file_modified

    from staging

),

{#
    Step 2: Correct DOUBLE FAULT rows that were incorrectly categorised as wins.
    If point_end_type = 'DOUBLE FAULT' and outcome = 'W', swap player/opponent and set outcome to 'L'.
#}

corrected as (

    select
        year,
        tournament_id,
        match_id,
        sets_completed,
        shot_number,

        case
            when point_end_type = 'DOUBLE FAULT' and outcome = 'W' then 'L'
            else outcome
        end as outcome,

        case
            when point_end_type = 'DOUBLE FAULT' and outcome = 'W' then opponent_id
            else player_id
        end as player_id,

        case
            when point_end_type = 'DOUBLE FAULT' and outcome = 'W' then player_id
            else opponent_id
        end as opponent_id,

        point_id,
        set_number,
        game,
        point,
        score,
        serve,
        serve_dir,
        court_side,
        serve_speed,
        hand,
        shot_type,
        point_end_type,
        crucial_point,
        t1_break_point,
        t2_break_point,
        t1_net_point,
        t2_net_point,
        tie_break,
        set_point,

        meta_file_name,
        meta_file_modified

    from enriched

),

{#
    Step 3: Deduplicate to the latest file version per match point.
#}

final as (

    select *
    from corrected
    qualify row_number() over (
        partition by year, tournament_id, match_id, point_id
        order by meta_file_modified desc
    ) = 1

)

select * from final
