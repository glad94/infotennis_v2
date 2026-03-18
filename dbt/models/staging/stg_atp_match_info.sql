with raw as (
    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_match_info') }}
),

base as (
    select
        json_extract(data, '$.data.Match') as payload,
        meta_file_name,
        meta_file_modified
    from raw
),

-- Use basic parsing to keep it clean, or we can just extract from raw.data.Match mapping manually below.
final as (
    select
        -- Match identification (parsed from S3 path)
        cast(regexp_extract(meta_file_name, 'year=(\d+)', 1) as int)    as year,
        regexp_extract(meta_file_name, 'tourn=([^/]+)', 1)              as tournament_id,
        regexp_extract(meta_file_name, '/([^/_]+)_\w+-vs-\w+', 1)       as match_id,
        
        -- File metadata
        meta_file_name,
        meta_file_modified,

        -- Match General Info
        json_extract_string(json_extract(data, '$.data.Match'), '$.CourtId') as match_courtid,
        json_extract_string(json_extract(data, '$.data.Match'), '$.Winner') as match_winner,
        json_extract_string(json_extract(data, '$.data.Match'), '$.ScoreCentreMatchSortOrder') as match_scorecentrematchsortorder,
        json_extract_string(json_extract(data, '$.data.Match'), '$.RightRailMatchSortOrder') as match_rightrailmatchsortorder,
        json_extract_string(json_extract(data, '$.data.Match'), '$.UmpireFirstName') as match_umpirefirstname,
        json_extract_string(json_extract(data, '$.data.Match'), '$.UmpireLastName') as match_umpirelastname,
        json_extract_string(json_extract(data, '$.data.Match'), '$.DurationInsideTheLines') as match_durationinsidethelines,
        json_extract_string(json_extract(data, '$.data.Match'), '$.DurationTotal') as match_durationtotal,

        -- Player1 Data

        -- Player1 Stats

        -- Player2 Data

        -- Player2 Stats

    from raw
)

select * from final
