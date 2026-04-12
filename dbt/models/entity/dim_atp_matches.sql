{#
    Dimension table for ATP matches.
    One row per match with players, result, round, score, duration, and umpire.

    Sources:
      - stg_atp_tournament_results: match_id, round, players (id/name/seed/nation), score
      - stg_atp_match_info:         winner, duration, umpire, court, number of sets, set scores
      - stg_atp_key_stats:          sets_completed (from set 1, since it's match-level)

    All ID and country columns are uppercased for consistency.
    Grain: one row per (year, tournament_id, match_id).
#}

{{
    config(
        materialized='table'
    )
}}

with results as (

    select
        year,
        tournament_name,
        tournament_id,
        round,
        upper(match_id)    as match_id,
        player1_name,
        upper(player1_id)  as player1_id,
        player1_seed,
        upper(player1_nation) as player1_nation,
        player2_name,
        upper(player2_id)  as player2_id,
        player2_seed,
        upper(player2_nation) as player2_nation,
        nullif(score, '')  as score,
        nullif(url, '')    as url,
        meta_file_modified
    from {{ ref('stg_atp_tournament_results') }}
    where nullif(match_id, '') is not null
    qualify row_number() over (
        partition by year, tournament_id, upper(match_id)
        order by meta_file_modified desc
    ) = 1

),

match_info as (

    select
        year,
        tournament_id,
        match_id,
        court_name,
        match_time,
        match_time_total,
        winner_id,
        number_of_sets,
        date_seq,
        round_name,
        match_status,
        umpire_first_name,
        umpire_last_name,
        player1_id          as mi_player1_id,
        player1_first_name  as mi_player1_first_name,
        player1_last_name   as mi_player1_last_name,
        player1_country_code as mi_player1_country_code,
        player2_id          as mi_player2_id,
        player2_first_name  as mi_player2_first_name,
        player2_last_name   as mi_player2_last_name,
        player2_country_code as mi_player2_country_code,
        player1_set_scores_json,
        player2_set_scores_json,
        meta_file_modified
    from {{ ref('stg_atp_match_info') }}
    where match_id is not null
    qualify row_number() over (
        partition by year, tournament_id, match_id
        order by meta_file_modified desc
    ) = 1

),

{# Get sets_completed from key_stats (same value across all sets for a match, so take from set 1) #}

key_stats_match as (

    select
        year,
        tournament_id,
        upper(match_id) as match_id,
        sets_completed,
        meta_file_modified
    from {{ ref('stg_atp_key_stats') }}
    where set_number = 1
    qualify row_number() over (
        partition by year, tournament_id, upper(match_id)
        order by meta_file_modified desc
    ) = 1

),

final as (

    select
        r.year,
        r.tournament_name,
        r.tournament_id,
        coalesce(mi.round_name, r.round) as round,
        r.match_id,

        -- Player 1
        coalesce(r.player1_id, mi.mi_player1_id) as player1_id,
        r.player1_name,
        r.player1_seed,
        coalesce(r.player1_nation, mi.mi_player1_country_code) as player1_nation,

        -- Player 2
        coalesce(r.player2_id, mi.mi_player2_id) as player2_id,
        r.player2_name,
        r.player2_seed,
        coalesce(r.player2_nation, mi.mi_player2_country_code) as player2_nation,

        -- Result
        r.score,
        mi.winner_id,
        coalesce(mi.number_of_sets, ks.sets_completed) as number_of_sets,
        ks.sets_completed,
        mi.match_status,

        -- Match context
        mi.match_time,
        mi.match_time_total,
        mi.court_name,
        mi.date_seq,
        case
            when mi.umpire_first_name is not null and mi.umpire_last_name is not null
            then mi.umpire_first_name || ' ' || mi.umpire_last_name
        end as umpire,

        -- Set scores (raw JSON arrays for flexible downstream use)
        mi.player1_set_scores_json,
        mi.player2_set_scores_json,

        r.url

    from results r
    left join match_info mi
        on r.year = mi.year
        and r.tournament_id = mi.tournament_id
        and r.match_id = mi.match_id
    left join key_stats_match ks
        on r.year = ks.year
        and r.tournament_id = ks.tournament_id
        and r.match_id = ks.match_id

)

select * from final
