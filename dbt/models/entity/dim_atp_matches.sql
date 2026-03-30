{#
    Dimension table for ATP matches.
    One row per match with players, result, round, score, duration, and umpire.

    Sources:
      - stg_atp_tournament_results: match_id, round, players (id/name/seed/nation), score
      - stg_atp_match_info:         winner, duration, umpire
      - stg_atp_key_stats:          sets_completed (from set 1 only, since it's match-level)

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
        match_id,
        player1_name,
        player1_id,
        player1_seed,
        player1_nation,
        player2_name,
        player2_id,
        player2_seed,
        player2_nation,
        score,
        url,
        meta_file_modified
    from {{ ref('stg_atp_tournament_results') }}
    where match_id is not null
    qualify row_number() over (
        partition by year, tournament_id, match_id
        order by meta_file_modified desc
    ) = 1

),

match_info as (

    select
        year,
        tournament_id,
        match_id,
        match_winner,
        match_durationtotal      as duration_total,
        match_durationinsidethelines as duration_inside_lines,
        match_umpirefirstname || ' ' || match_umpirelastname as umpire,
        match_courtid            as court_id,
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
        match_id,
        sets_completed,
        meta_file_modified
    from {{ ref('stg_atp_key_stats') }}
    where set_number = 1
    qualify row_number() over (
        partition by year, tournament_id, match_id
        order by meta_file_modified desc
    ) = 1

),

final as (

    select
        r.year,
        r.tournament_name,
        r.tournament_id,
        r.round,
        r.match_id,

        -- Player 1
        r.player1_id,
        r.player1_name,
        r.player1_seed,
        r.player1_nation,

        -- Player 2
        r.player2_id,
        r.player2_name,
        r.player2_seed,
        r.player2_nation,

        -- Result
        r.score,
        mi.match_winner as winner_id,
        ks.sets_completed,

        -- Match context
        mi.duration_total,
        mi.duration_inside_lines,
        mi.umpire,
        mi.court_id,
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
