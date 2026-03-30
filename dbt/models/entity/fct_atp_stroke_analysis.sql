{#
    Entity fact table for ATP Stroke Analysis.
    Translates the Python processing_strokes.py logic into SQL.

    The staging model provides point-level rows from rallyShots.allPoints.
    This entity model:
      - Classifies point_end_type into outcome categories
        (winner, forced_error, unforced_error, other)
      - Extracts game and point from point_id (format: set_game_point)
      - Deduplicates to the latest file version per match point
#}

{{
    config(
        materialized='incremental',
        unique_key=['year', 'tournament_id', 'match_id', 'point_id']
    )
}}

with staging as (

    select * from {{ ref('stg_atp_stroke_analysis') }}
    {% if is_incremental() %}
    where meta_file_modified > (select max(meta_file_modified) from {{ this }})
    {% endif %}

),

enriched as (

    select
        -- Match identification
        year,
        tournament_id,
        match_id,
        sets_completed,

        -- Player info
        player1_id,
        player1_name,
        player1_country,
        player2_id,
        player2_name,
        player2_country,

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
        rally_length,

        -- Outcome category (mirrors Python's winners/errors/unforced_errors/others)
        case
            when upper(point_end_type) = 'WINNER'         then 'winner'
            when upper(point_end_type) = 'FORCED ERROR'    then 'forced_error'
            when upper(point_end_type) = 'UNFORCED ERROR'  then 'unforced_error'
            when upper(point_end_type) = 'DOUBLE FAULT'    then 'double_fault'
            when upper(point_end_type) = 'ACE'             then 'ace'
            else 'other'
        end as outcome_category,

        -- Point flags
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
    Deduplicate to the latest file version per match point.
#}

final as (

    select *
    from enriched
    qualify row_number() over (
        partition by year, tournament_id, match_id, point_id
        order by meta_file_modified desc
    ) = 1

)

select * from final
