{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Key Stats (Infosys API).
    Each row represents one match's key stats payload.
    Extracts match identification fields and top-level stats metadata.
    Per-set stats remain as JSON for downstream parsing.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_key_stats') }}

),

parsed as (

    select
        -- Retrieved timestamp
        json_extract_string(data, '$.metadata.retrieved_at')                as retrieved_at,

        -- Top-level match metadata from the stats payload
        cast(json_extract(data, '$.data.setsCompleted') as int)             as sets_completed,
        json_extract(data, '$.data.players')                                as players,

        -- Per-set stats (JSON objects)
        json_extract(data, '$.data.setStats')                               as set_stats,

        -- File metadata
        meta_file_name,
        meta_file_modified
    from raw

)

select * from parsed
