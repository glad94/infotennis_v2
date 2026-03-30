{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Court Vision (Infosys API).
    One row per point. The points data resides in the dictionary 'a50' 
    (where keys are point IDs like "1_2_4_2"). 
    Columns are kept as encoded 'a<numeric>' formats, with nested 
    dimensions (e.g. trajectories in a12, a27-a35) remaining JSON.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_court_vision') }}

),

base as (

    select
        case
            when json_type(json_extract(data, '$.data.courtVisionData')) = 'ARRAY'
            then json_extract(data, '$.data.courtVisionData[0]')
            else coalesce(json_extract(data, '$.data.courtVisionData'), json_extract(data, '$.data'))
        end as payload,
        meta_file_name,
        meta_file_modified
    from raw

),

points_raw as (

    select
        b.payload,
        b.meta_file_name,
        b.meta_file_modified,
        json_extract(b.payload, '$.a50') as a50_dict
    from base as b
    where b.payload is not null

),

points_unnested as (

    select
        pr.payload,
        pr.meta_file_name,
        pr.meta_file_modified,
        json_extract(pr.a50_dict, '$."' || k.key || '"') as pt
    from points_raw as pr,
    unnest(json_keys(pr.a50_dict)) as k(key)

),

parsed as (

    select
        -- Match identification (parsed from S3 path)
        cast(regexp_extract(meta_file_name, 'year=(\d+)', 1) as int) as year,
        regexp_extract(meta_file_name, 'tourn=([^/]+)', 1) as tournament_id,
        regexp_extract(meta_file_name, '/([^/_]+)_\w+-vs-\w+', 1) as match_id,  -- Handles new MS001_P1-vs-P2 format
        -- a79 Player Information (nested JSON arrays to be parsed in entity layer)
        json_extract(payload, '$.a79.a83') as a83,
        json_extract(payload, '$.a79.a84') as a84,
        -- a50 Point-level flattened properties (a<numeric>)
        json_extract_string(pt, '$.a11') as a11,
        json_extract(pt, '$.a12') as a12,
        json_extract_string(pt, '$.a13') as a13,
        json_extract_string(pt, '$.a14') as a14,
        json_extract_string(pt, '$.a15') as a15,
        json_extract_string(pt, '$.a16') as a16,
        json_extract_string(pt, '$.a17') as a17,
        json_extract_string(pt, '$.a18') as a18,
        json_extract_string(pt, '$.a19') as a19,
        json_extract_string(pt, '$.a20') as a20,
        json_extract_string(pt, '$.a21') as a21,
        json_extract_string(pt, '$.a22') as a22,
        json_extract_string(pt, '$.a23') as a23,
        json_extract_string(pt, '$.a24') as a24,
        json_extract_string(pt, '$.a25') as a25,
        json_extract_string(pt, '$.a26') as a26,
        json_extract(pt, '$.a27') as a27,
        json_extract(pt, '$.a28') as a28,
        json_extract(pt, '$.a29') as a29,
        json_extract(pt, '$.a30') as a30,
        json_extract(pt, '$.a31') as a31,
        json_extract(pt, '$.a32') as a32,
        json_extract(pt, '$.a33') as a33,
        json_extract(pt, '$.a34') as a34,
        json_extract(pt, '$.a35') as a35,
        json_extract_string(pt, '$.a36') as a36,
        json_extract_string(pt, '$.a81') as a81,
        json_extract_string(pt, '$.a86') as a86,
        json_extract_string(pt, '$.a89') as a89,
        json_extract_string(pt, '$.a90') as a90,
        json_extract_string(pt, '$.a91') as a91,
        json_extract_string(pt, '$.a92') as a92,
        json_extract_string(pt, '$.a93') as a93,
        json_extract_string(pt, '$.a94') as a94,
        json_extract_string(pt, '$.a95') as a95,
        json_extract_string(pt, '$.a96') as a96,
        json_extract_string(pt, '$.a97') as a97,
        json_extract_string(pt, '$.a98') as a98,
        json_extract_string(pt, '$.a99') as a99,
        json_extract_string(pt, '$.a100') as a100,
        json_extract_string(pt, '$.a101') as a101,
        json_extract_string(pt, '$.a102') as a102,
        json_extract_string(pt, '$.a103') as a103,
        json_extract_string(pt, '$.a104') as a104,
        json_extract_string(pt, '$.a105') as a105,
        json_extract_string(pt, '$.a106') as a106,
        json_extract_string(pt, '$.a107') as a107,
        json_extract_string(pt, '$.a108') as a108,
        json_extract_string(pt, '$.a109') as a109,
        json_extract_string(pt, '$.a149') as a149,
        -- File metadata
        meta_file_name,
        meta_file_modified

    from points_unnested

)

select * from parsed
