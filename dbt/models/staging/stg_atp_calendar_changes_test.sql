{{
    config(
        materialized='view'
    )
}}

{#
    Identifies tournaments that need their results page scraped.
    Two cases:
      1. Newly completed: url transitioned from NULL to non-NULL between the
         two most recent file versions.
      2. Currently ongoing: the latest url contains '/current/' (the 3rd or
         4th path segment, e.g. /en/scores/current/doha/451/results),
         indicating the tournament is live and has new results to obtain.
#}

with file_versions as (

    -- Rank distinct file versions by recency
    select distinct
        meta_file_modified,
        dense_rank() over (order by meta_file_modified desc) as version_rank
    from {{ ref('stg_atp_calendar_test') }}

),

current_version as (

    select cal.*
    from {{ ref('stg_atp_calendar_test') }} as cal
    inner join file_versions as fv
        on cal.meta_file_modified = fv.meta_file_modified
    where fv.version_rank = 1

),

previous_version as (

    select cal.*
    from {{ ref('stg_atp_calendar_test') }} as cal
    inner join file_versions as fv
        on cal.meta_file_modified = fv.meta_file_modified
    where fv.version_rank = 2

),

-- Case 1: Tournaments whose url changed from NULL to non-NULL
newly_completed as (

    select
        curr.year,
        curr.tournament,
        curr.tournament_id,
        curr.category,
        curr.city,
        curr.country,
        curr.dates,
        curr.singles_winner,
        curr.doubles_winner,
        curr.url,
        curr.meta_file_name,
        curr.meta_file_modified,
        prev.meta_file_modified as previous_meta_file_modified,
        'newly_completed' as change_type
    from current_version as curr
    inner join previous_version as prev
        on curr.tournament_id = prev.tournament_id
    where prev.url is null
      and curr.url is not null

),

-- Case 2: Tournaments with '/current/' in url (still ongoing)
currently_ongoing as (

    select
        curr.year,
        curr.tournament,
        curr.tournament_id,
        curr.category,
        curr.city,
        curr.country,
        curr.dates,
        curr.singles_winner,
        curr.doubles_winner,
        curr.url,
        curr.meta_file_name,
        curr.meta_file_modified,
        null as previous_meta_file_modified,
        'ongoing' as change_type
    from current_version as curr
    where curr.url is not null
      and split_part(curr.url, '/', 4) = 'current'

),

combined as (

    select * from newly_completed
    union all
    select * from currently_ongoing
    where tournament_id not in (select tournament_id from newly_completed)

)

select * from combined

