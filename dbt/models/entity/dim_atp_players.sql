{#
    Dimension table for ATP players.
    Derives unique player records from staging models that contain player metadata.

    Sources:
      - stg_atp_key_stats: player_id, name (full/first/last), country
      - stg_atp_tournament_results: player_id, name, nation

    Deduplicates by player_id, keeping the most recently seen record.
#}

{{
    config(
        materialized='table'
    )
}}

with key_stats_players as (

    -- Player 1 from key stats
    select distinct
        player1_id        as player_id,
        player1_name      as player_name,
        player1_first_name as first_name,
        player1_last_name  as last_name,
        player1_country   as country,
        meta_file_modified
    from {{ ref('stg_atp_key_stats') }}
    where player1_id is not null

    union all

    -- Player 2 from key stats
    select distinct
        player2_id        as player_id,
        player2_name      as player_name,
        player2_first_name as first_name,
        player2_last_name  as last_name,
        player2_country   as country,
        meta_file_modified
    from {{ ref('stg_atp_key_stats') }}
    where player2_id is not null

),

tournament_players as (

    -- Player 1 from tournament results
    select distinct
        player1_id        as player_id,
        player1_name      as player_name,
        null              as first_name,
        null              as last_name,
        player1_nation    as country,
        meta_file_modified
    from {{ ref('stg_atp_tournament_results') }}
    where player1_id is not null

    union all

    -- Player 2 from tournament results
    select distinct
        player2_id        as player_id,
        player2_name      as player_name,
        null              as first_name,
        null              as last_name,
        player2_nation    as country,
        meta_file_modified
    from {{ ref('stg_atp_tournament_results') }}
    where player2_id is not null

),

all_players as (

    select * from key_stats_players
    union all
    select * from tournament_players

),

{#
    Deduplicate: keep the most recent record per player_id,
    preferring rows that have first/last name populated.
#}

final as (

    select
        player_id,
        player_name,
        first_name,
        last_name,
        country
    from all_players
    qualify row_number() over (
        partition by player_id
        order by
            case when first_name is not null then 0 else 1 end,
            meta_file_modified desc
    ) = 1

)

select * from final
