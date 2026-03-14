{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Match Info (Hawkeye API).
    Each row represents one match's complete info payload.
    Extracts match identification and stores the full data JSON for downstream use.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_match_info') }}

),

parsed as (

    select
        -- Match identification (from metadata stored in the wrapper)
        json_extract_string(data, '$.metadata.retrieved_at')                as retrieved_at,

        -- The actual match info payload
        json_extract(data, '$.data')                                        as match_data,

        -- File metadata
        meta_file_name,
        meta_file_modified
    from raw

)

select * from parsed
