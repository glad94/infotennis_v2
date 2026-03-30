{#
    Dimension table for ATP tournaments.
    Combines tournament metadata from the ATP tournaments API endpoint
    with calendar data (year, category, city, country, dates, winners).

    Sources:
      - stg_atp_tournaments: surface, draw sizes, prize money, indoor/outdoor, location
      - stg_atp_calendar:    year, category, city, country, dates, singles/doubles winner

    Grain: one row per (year, tournament_id).
#}

{{
    config(
        materialized='table'
    )
}}

with tournaments as (

    select
        tournament_id,
        tournament_name,
        location,
        surface,
        indoor_outdoor,
        total_financial_commitment,
        singles_draw_size,
        doubles_draw_size,
        event_type
    from {{ ref('stg_atp_tournaments') }}
    where tournament_id is not null

),

calendar as (

    select
        year,
        tournament_id,
        tournament   as tournament_name_calendar,
        category,
        city,
        country,
        dates,
        singles_winner,
        doubles_winner,
        meta_file_modified
    from {{ ref('stg_atp_calendar') }}
    where tournament_id is not null

),

{#
    Calendar has one row per (year, tournament_id) per file version.
    Deduplicate to the latest file version.
#}

calendar_deduped as (

    select *
    from calendar
    qualify row_number() over (
        partition by year, tournament_id
        order by meta_file_modified desc
    ) = 1

),

{#
    Join calendar (per-year) with tournaments (static metadata).
    Calendar is the base since it has year granularity.
    Tournaments from the API may not cover all years (it's a current-season snapshot).
#}

final as (

    select
        c.year,
        c.tournament_id,
        coalesce(t.tournament_name, c.tournament_name_calendar) as tournament_name,
        c.category,
        c.city,
        c.country,
        c.dates,
        t.surface,
        t.indoor_outdoor,
        t.total_financial_commitment,
        t.singles_draw_size,
        t.doubles_draw_size,
        t.event_type,
        t.location,
        c.singles_winner,
        c.doubles_winner
    from calendar_deduped c
    left join tournaments t
        on c.tournament_id = t.tournament_id

)

select * from final
