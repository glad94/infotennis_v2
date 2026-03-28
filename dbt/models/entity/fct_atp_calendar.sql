with staging as (

    select *
    from {{ ref('stg_atp_calendar') }}

),

deduped as (

    select *
    from staging
    qualify row_number() over (
        partition by year, tournament_id
        order by meta_file_modified desc
    ) = 1

),

final as (

    select
        -- Identifiers
        year,
        tournament_id,
        tournament as tournament_name,

        -- Location and Meta
        category,
        city,
        country,
        url,

        -- Parsed Dates
        try_strptime(split_part(dates, ' - ', 1), '%Y.%m.%d')::date as start_date,
        try_strptime(split_part(dates, ' - ', 2), '%Y.%m.%d')::date as end_date,

        -- Winners
        singles_winner,
        doubles_winner

    from deduped

)

select * from final
