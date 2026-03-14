{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Court Vision (Infosys API).
    Each row represents one match's court vision payload.

    The raw court vision data uses encoded column names (a11, a12, ...).
    This staging layer renames the top-level keys of the courtVisionData
    object and stores the decoded key references for downstream use.

    Column name mappings come from the original infotennis pipeline:
      dict_cols      — main point-level fields (a11→cruciality, a81→point_id, etc.)
      matchScore_cols — score fields within each point (a122→p1_set1_score, etc.)
      coordinate cols — a70→x, a71→y, a72→z, a73→position, a74→erroneous_ball

    Only JSON field extraction is performed here — no trajectory expansion,
    no score derivation, no type casting.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_court_vision') }}

),

parsed as (

    select
        -- Retrieved timestamp
        json_extract_string(data, '$.metadata.retrieved_at')                as retrieved_at,

        -- Court vision data array (contains the encoded point-level data)
        json_extract(data, '$.data.courtVisionData')                        as court_vision_data,

        -- File metadata
        meta_file_name,
        meta_file_modified
    from raw

)

select * from parsed
