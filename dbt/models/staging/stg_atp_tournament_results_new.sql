{{
    config(
        materialized='view'
    )
}}

{#
    Tracks "new" tournament results that were added in the most recent load
    for each tournament. Compares the current file version against the previous
    one per tournament_id to identify newly scraped matches.
    This informs what match-level data to retrieve next.
#}

with file_versions_per_tournament as (

    -- For each tournament, rank file versions by recency
    select distinct
        tournament_id,
        meta_file_modified,
        dense_rank() over (
            partition by tournament_id
            order by meta_file_modified desc
        ) as version_rank
    from {{ ref('stg_atp_tournament_results') }}

),

current_matches as (

    select r.*
    from {{ ref('stg_atp_tournament_results') }} as r
    inner join file_versions_per_tournament as fv
        on r.tournament_id = fv.tournament_id
       and r.meta_file_modified = fv.meta_file_modified
    where fv.version_rank = 1

),

previous_matches as (

    select r.*
    from {{ ref('stg_atp_tournament_results') }} as r
    inner join file_versions_per_tournament as fv
        on r.tournament_id = fv.tournament_id
       and r.meta_file_modified = fv.meta_file_modified
    where fv.version_rank = 2

),

new_results as (

    select
        curr.year,
        curr.tournament_name,
        curr.tournament_id,
        curr.round,
        curr.match_id,
        curr.player1_name,
        curr.player1_id,
        curr.player2_name,
        curr.player2_id,
        curr.score,
        curr.url,
        curr.meta_file_name,
        curr.meta_file_modified
    from current_matches as curr
    left join previous_matches as prev
        on curr.tournament_id = prev.tournament_id
       and curr.match_id = prev.match_id
    where prev.match_id is null

)

select * from new_results
