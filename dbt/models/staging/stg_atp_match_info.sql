{#
    Staging view for ATP Match Info (from Hawkeye API).
    Parses the JSON payload under data.Match into a flat table.
    One row per file load (one match per file).

    All ID and country columns are uppercased for consistent joins.
    Blank strings are converted to NULL.
#}

with raw as (
    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_match_info') }}
),

final as (
    select
        -- Match identification (year/tournament from S3 path, match_id from JSON payload)
        cast(regexp_extract(meta_file_name, 'year=(\d+)', 1) as int)    as year,
        regexp_extract(meta_file_name, 'tourn=([^/]+)', 1)              as tournament_id,
        nullif(upper(json_extract_string(data, '$.data.Match.MatchId')), '') as match_id,

        -- Match general info
        nullif(json_extract_string(data, '$.data.Match.CourtName'), '')            as court_name,
        nullif(json_extract_string(data, '$.data.Match.CourtId'), '')              as court_id,
        nullif(json_extract_string(data, '$.data.Match.MatchTime'), '')            as match_time,
        nullif(json_extract_string(data, '$.data.Match.MatchTimeTotal'), '')       as match_time_total,
        nullif(upper(json_extract_string(data, '$.data.Match.Winner')), '')        as winner_id,
        nullif(upper(json_extract_string(data, '$.data.Match.WinningPlayerId')), '') as winning_player_id,
        try_cast(json_extract(data, '$.data.Match.NumberOfSets') as int)           as number_of_sets,
        nullif(json_extract_string(data, '$.data.Match.DateSeq'), '')              as date_seq,
        nullif(json_extract_string(data, '$.data.Match.RoundName'), '')            as round_name,
        nullif(json_extract_string(data, '$.data.Match.MatchStatus'), '')          as match_status,
        nullif(json_extract_string(data, '$.data.Match.UmpireFirstName'), '')      as umpire_first_name,
        nullif(json_extract_string(data, '$.data.Match.UmpireLastName'), '')       as umpire_last_name,

        -- Player 1 info (from PlayerTeam1)
        nullif(upper(json_extract_string(data, '$.data.Match.PlayerTeam1.PlayerId')), '')            as player1_id,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam1.PlayerFirstNameFull'), '')        as player1_first_name,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam1.PlayerLastName'), '')             as player1_last_name,
        nullif(upper(json_extract_string(data, '$.data.Match.PlayerTeam1.PlayerCountryCode')), '')   as player1_country_code,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam1.SeedPlayerTeam'), '')             as player1_seed,

        -- Player 2 info (from PlayerTeam2)
        nullif(upper(json_extract_string(data, '$.data.Match.PlayerTeam2.PlayerId')), '')            as player2_id,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam2.PlayerFirstNameFull'), '')        as player2_first_name,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam2.PlayerLastName'), '')             as player2_last_name,
        nullif(upper(json_extract_string(data, '$.data.Match.PlayerTeam2.PlayerCountryCode')), '')   as player2_country_code,
        nullif(json_extract_string(data, '$.data.Match.PlayerTeam2.SeedPlayerTeam'), '')             as player2_seed,

        -- Set scores as JSON dict (variable number of sets)
        json_extract(data, '$.data.Match.PlayerTeam.SetScores')         as player1_set_scores_json,
        json_extract(data, '$.data.Match.OpponentTeam.SetScores')       as player2_set_scores_json,

        -- File metadata
        meta_file_name,
        meta_file_modified

    from raw
)

select * from final
