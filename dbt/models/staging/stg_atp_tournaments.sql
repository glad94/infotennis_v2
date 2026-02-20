{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Tournaments.
    Flattens the nested JSON (TournamentDates -> Tournaments) from the
    most recent file load only, since this data is static and refreshed
    infrequently.
#}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_tournaments') }}
    qualify row_number() over (order by meta_file_modified desc) = 1

),

date_groups as (

    select
        dg.json as date_group,
        raw.meta_file_name,
        raw.meta_file_modified
    from raw,
         lateral unnest(
             cast(json_extract(raw.data, '$.data.TournamentDates') as json[])
         ) as dg(json)

),

flattened as (

    select
        json_extract_string(t.json, '$.Id')                          as tournament_id,
        json_extract_string(t.json, '$.Name')                        as tournament_name,
        json_extract_string(t.json, '$.Location')                    as location,
        json_extract_string(t.json, '$.FormattedDate')               as formatted_date,
        json_extract_string(t.json, '$.Surface')                     as surface,
        json_extract_string(t.json, '$.IndoorOutdoor')               as indoor_outdoor,
        json_extract_string(t.json, '$.TotalFinancialCommitment')    as total_financial_commitment,
        json_extract_string(t.json, '$.PrizeMoneyDetails')           as prize_money_details,
        cast(json_extract(t.json, '$.SglDrawSize') as int)           as singles_draw_size,
        cast(json_extract(t.json, '$.DblDrawSize') as int)           as doubles_draw_size,
        json_extract_string(t.json, '$.Type')                        as event_type,
        json_extract_string(t.json, '$.ChallengerCategory')          as challenger_category,
        json_extract_string(t.json, '$.TournamentOverviewUrl')       as tournament_overview_url,
        json_extract_string(t.json, '$.TournamentSiteUrl')           as tournament_site_url,
        json_extract_string(t.json, '$.ScoresUrl')                   as scores_url,
        cast(json_extract(t.json, '$.IsLive') as boolean)            as is_live,
        cast(json_extract(t.json, '$.IsPastEvent') as boolean)       as is_past_event,
        dg.meta_file_name,
        dg.meta_file_modified
    from date_groups as dg,
         lateral unnest(
             cast(json_extract(dg.date_group, '$.Tournaments') as json[])
         ) as t(json)

)

select * from flattened
