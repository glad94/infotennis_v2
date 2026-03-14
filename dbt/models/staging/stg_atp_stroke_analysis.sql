{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Stroke Analysis (Infosys API).
    Each row represents one match's stroke analysis payload.
    Extracts top-level metadata; forehand/backhand shot outcomes remain as JSON.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_stroke_analysis') }}

),

parsed as (

    select
        -- Retrieved timestamp
        json_extract_string(data, '$.metadata.retrieved_at')                as retrieved_at,

        -- Top-level match metadata
        cast(json_extract(data, '$.data.setsCompleted') as int)             as sets_completed,
        json_extract(data, '$.data.players')                                as players,

        -- Stroke data (forehand/backhand breakdown per set)
        json_extract(data, '$.data.rallyShots')                             as rally_shots,

        -- File metadata
        meta_file_name,
        meta_file_modified
    from raw

)

select * from parsed
