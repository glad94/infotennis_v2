{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Rally Analysis (Infosys API).
    Each row represents one match's rally analysis payload.
    Extracts top-level metadata; point-level rally data remains as JSON.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_rally_analysis') }}

),

parsed as (

    select
        -- Retrieved timestamp
        json_extract_string(data, '$.metadata.retrieved_at')                as retrieved_at,

        -- Top-level match metadata
        cast(json_extract(data, '$.data.setsCompleted') as int)             as sets_completed,
        json_extract(data, '$.data.playerDetails')                          as player_details,

        -- Rally data (point-by-point with shot outcomes)
        json_extract(data, '$.data.rallyData')                              as rally_data,

        -- File metadata
        meta_file_name,
        meta_file_modified
    from raw

)

select * from parsed
