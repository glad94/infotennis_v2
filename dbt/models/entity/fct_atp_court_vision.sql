{#
    Entity fact table for ATP Court Vision.
    Translates the Python processing_courtvision.py logic into SQL.

    Transformations:
      - Renames a* coded columns to human-readable names
      - Explodes trajectory data (a12) from 1-row-per-point to 1-row-per-shot
      - Pivots ball coordinates to wide format with x/y swap
        (y=0 at net, x=left/right from behind-baseline perspective)
      - ball_speed_kmh only populated on stroke_idx=1 (the serve), NULL on faulty serves
      - Match score state derived (sets won, set/game scores, tiebreak flag)
      - NULL for missing values (no -999 sentinels)
      - hand/stroke_type only on last shot; serve_type NULL on faulty serves
      - Deduplicates to latest file version per match point + stroke
#}

{{
    config(
        materialized='incremental',
        unique_key=['year', 'tournament_id', 'match_id', 'point_id', 'stroke_idx']
    )
}}

with staging as (

    select * from {{ ref('stg_atp_court_vision') }}
    {% if is_incremental() %}
    where meta_file_modified > (select max(meta_file_modified) from {{ this }})
    {% endif %}

),

{# Step 1: Rename a* columns, parse types. Keep trajectory + score JSON for downstream CTEs. #}

renamed as (

    select
        year,
        tournament_id,
        upper(match_id) as match_id,
        upper(json_extract_string(a83, '$[0].a86')) as player1_id,
        upper(json_extract_string(a84, '$[0].a86')) as player2_id,
        a81 as point_id,

        upper(nullif(a13, 'NA')) as server_id,
        upper(nullif(a14, 'NA')) as scorer_id,
        upper(nullif(a15, 'NA')) as receiver_id,
        try_cast(split_part(coalesce(nullif(a21, 'NA'), ''), ' ', 1) as double) as ball_speed_kmh,
        try_cast(nullif(a93, 'NA') as int) as rally_length,
        nullif(a95, 'NA') as point_end_type,
        nullif(a90, 'NA') as error_type,
        nullif(a25, 'NA') as stroke_type,
        nullif(a96, 'NA') as serve_type,
        nullif(a97, 'NA') as court,
        try_cast(nullif(a98, 'NA') as int) as set_n,
        try_cast(nullif(a100, 'NA') as int) as game,
        try_cast(nullif(a101, 'NA') as int) as point,
        try_cast(nullif(a102, 'NA') as int) as serve,
        nullif(a103, 'NA') as hand,
        nullif(a104, 'NA') as break_point,
        nullif(a105, 'NA') as break_point_converted,

        a12 as trajectory_data,
        a35 as match_score_json,
        meta_file_modified
    from staging
    where a81 is not null

),

{# Step 2: Unnest trajectory — only carry join keys + coordinates to keep lightweight #}

trajectory_unnested as (

    select
        r.year, r.tournament_id, r.match_id, r.point_id, r.meta_file_modified,
        t.traj_idx,
        json_extract_string(t.elem, '$.a73') as position,
        try_cast(json_extract_string(t.elem, '$.a70') as double) as raw_a70,
        try_cast(json_extract_string(t.elem, '$.a71') as double) as raw_a71,
        try_cast(json_extract_string(t.elem, '$.a72') as double) as raw_a72
    from renamed r,
    lateral (
        select
            unnest(cast(r.trajectory_data as json[])) as elem,
            unnest(generate_series(1::bigint, json_array_length(r.trajectory_data)::bigint)) as traj_idx
    ) as t
    where r.trajectory_data is not null
      and json_array_length(r.trajectory_data) > 0

),

{# Step 3: Assign stroke_idx #}

trajectory_stroked as (

    select *,
        sum(case when position = 'hit' then 1 else 0 end) over (
            partition by year, tournament_id, match_id, point_id, meta_file_modified
            order by traj_idx
        ) as stroke_idx
    from trajectory_unnested

),

{# Step 4: Pivot to wide format with peak labeling + x/y swap in one step #}

trajectory_wide as (

    select
        year, tournament_id, match_id, point_id, meta_file_modified,
        stroke_idx,

        max(case when position = 'hit' then raw_a71 end) as x_hit,
        max(case when position = 'hit' then raw_a70 end) as y_hit,
        max(case when position = 'hit' then raw_a72 end) as z_hit,

        max(case when position = 'peak' and peak_num = 1 then raw_a71 end) as x_peak_before_net,
        max(case when position = 'peak' and peak_num = 1 then raw_a70 end) as y_peak_before_net,
        max(case when position = 'peak' and peak_num = 1 then raw_a72 end) as z_peak_before_net,

        max(case when position = 'net' then raw_a71 end) as x_net,
        max(case when position = 'net' then raw_a70 end) as y_net,
        max(case when position = 'net' then raw_a72 end) as z_net,

        max(case when position = 'bounce' then raw_a71 end) as x_bounce,
        max(case when position = 'bounce' then raw_a70 end) as y_bounce,
        max(case when position = 'bounce' then raw_a72 end) as z_bounce,

        max(case when position = 'peak' and peak_num = 2 then raw_a71 end) as x_peak_after_net,
        max(case when position = 'peak' and peak_num = 2 then raw_a70 end) as y_peak_after_net,
        max(case when position = 'peak' and peak_num = 2 then raw_a72 end) as z_peak_after_net

    from (
        select *,
            case when position = 'peak'
                then sum(case when position = 'peak' then 1 else 0 end) over (
                    partition by year, tournament_id, match_id, point_id, meta_file_modified, stroke_idx
                    order by traj_idx
                    rows between unbounded preceding and current row
                )
            end as peak_num
        from trajectory_stroked
    )
    group by year, tournament_id, match_id, point_id, meta_file_modified, stroke_idx

),

{# Step 5: Match score — separate CTE to avoid bloating renamed #}

match_score as (

    select
        year, tournament_id, match_id, point_id, meta_file_modified,

        json_extract_string(match_score_json, '$.a142') as p1_game_score,
        json_extract_string(match_score_json, '$.a143') as p2_game_score,

        try_cast(json_extract_string(match_score_json,
            '$.a' || cast(121 + set_n as varchar)) as int) as p1_current_set_score,
        try_cast(json_extract_string(match_score_json,
            '$.a' || cast(131 + set_n as varchar)) as int) as p2_current_set_score,

        coalesce(case when set_n > 1 and try_cast(json_extract_string(match_score_json, '$.a122') as int) > try_cast(json_extract_string(match_score_json, '$.a132') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 2 and try_cast(json_extract_string(match_score_json, '$.a123') as int) > try_cast(json_extract_string(match_score_json, '$.a133') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 3 and try_cast(json_extract_string(match_score_json, '$.a124') as int) > try_cast(json_extract_string(match_score_json, '$.a134') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 4 and try_cast(json_extract_string(match_score_json, '$.a125') as int) > try_cast(json_extract_string(match_score_json, '$.a135') as int) then 1 else 0 end, 0)
        as p1_sets_won,

        coalesce(case when set_n > 1 and try_cast(json_extract_string(match_score_json, '$.a132') as int) > try_cast(json_extract_string(match_score_json, '$.a122') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 2 and try_cast(json_extract_string(match_score_json, '$.a133') as int) > try_cast(json_extract_string(match_score_json, '$.a123') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 3 and try_cast(json_extract_string(match_score_json, '$.a134') as int) > try_cast(json_extract_string(match_score_json, '$.a124') as int) then 1 else 0 end, 0)
        + coalesce(case when set_n > 4 and try_cast(json_extract_string(match_score_json, '$.a135') as int) > try_cast(json_extract_string(match_score_json, '$.a125') as int) then 1 else 0 end, 0)
        as p2_sets_won,

        case when tournament_id = '7696' then 3 else 6 end as score_tb,
        case when tournament_id = '7696' then 4 else 7 end as score_tb_win

    from renamed
    where match_score_json is not null

),

match_score_final as (

    select
        year, tournament_id, match_id, point_id, meta_file_modified,
        p1_game_score, p2_game_score,
        p1_current_set_score, p2_current_set_score,
        p1_sets_won, p2_sets_won,
        case
            when (p1_current_set_score = score_tb_win and p2_current_set_score = score_tb)
              or (p1_current_set_score = score_tb and p2_current_set_score = score_tb_win)
            then 1
            when p1_current_set_score = score_tb and p2_current_set_score = score_tb
              and coalesce(p1_game_score, '') != 'GAME'
              and coalesce(p2_game_score, '') != 'GAME'
            then 1
            else 0
        end as is_tiebreak
    from match_score

),

{# Step 6: Join point-level with trajectory and match score, deduplicate #}

joined as (

    select
        r.year,
        r.tournament_id,
        r.match_id,
        r.player1_id,
        r.player2_id,
        r.point_id,
        r.server_id,
        r.scorer_id,
        r.receiver_id,

        case
            when coalesce(tw.stroke_idx, 1) = 1
                 and r.point_end_type != 'Faulty Serve'
            then r.ball_speed_kmh
        end as ball_speed_kmh,

        r.rally_length,
        r.point_end_type,
        r.error_type,

        case
            when tw.stroke_idx = r.rally_length
              or tw.stroke_idx = r.rally_length + 1
            then r.stroke_type
        end as stroke_type,

        case
            when r.point_end_type != 'Faulty Serve'
            then r.serve_type
        end as serve_type,

        r.court,
        r.set_n,
        r.game,
        r.point,
        r.serve,

        case
            when tw.stroke_idx = r.rally_length
              or tw.stroke_idx = r.rally_length + 1
            then r.hand
        end as hand,

        r.break_point,
        r.break_point_converted,

        ms.p1_sets_won,
        ms.p2_sets_won,
        ms.p1_current_set_score,
        ms.p2_current_set_score,
        ms.p1_game_score,
        ms.p2_game_score,
        ms.is_tiebreak,

        tw.stroke_idx,
        tw.x_hit, tw.y_hit, tw.z_hit,
        tw.x_peak_before_net, tw.y_peak_before_net, tw.z_peak_before_net,
        tw.x_net, tw.y_net, tw.z_net,
        tw.x_bounce, tw.y_bounce, tw.z_bounce,
        tw.x_peak_after_net, tw.y_peak_after_net, tw.z_peak_after_net,

        r.meta_file_modified
    from renamed r
    left join trajectory_wide tw
        on r.year = tw.year
        and r.tournament_id = tw.tournament_id
        and r.match_id = tw.match_id
        and r.point_id = tw.point_id
        and r.meta_file_modified = tw.meta_file_modified
    left join match_score_final ms
        on r.year = ms.year
        and r.tournament_id = ms.tournament_id
        and r.match_id = ms.match_id
        and r.point_id = ms.point_id
        and r.meta_file_modified = ms.meta_file_modified

),

final as (

    select *
    from joined
    qualify row_number() over (
        partition by year, tournament_id, match_id, point_id, stroke_idx
        order by meta_file_modified desc
    ) = 1

)

select * from final
