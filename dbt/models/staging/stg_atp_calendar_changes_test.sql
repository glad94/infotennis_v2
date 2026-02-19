{{
    config(
        materialized='view'
    )
}}

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
        prev.meta_file_modified as previous_meta_file_modified
    from current_version as curr
    inner join previous_version as prev
        on curr.tournament_id = prev.tournament_id
    where prev.url is null
      and curr.url is not null

)

select * from newly_completed
