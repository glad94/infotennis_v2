{{
    config(
        materialized='view'
    )
}}

{#
    Staging view for ATP Key Stats (Infosys API).
    One row per (match, set).  Sets 0-5 are unnested using generate_series.
    Stats are extracted by matching on the "name" field within each set's
    JSON array, so the model is resilient to ordering changes.

    Values are left in their raw string format (e.g. "30/51 (59%)").
#}

{% set stats = [
    ("Serve Rating",               "serve_rating"),
    ("Aces",                       "aces"),
    ("Double Faults",              "double_faults"),
    ("1st Serve",                  "first_serve"),
    ("1st Serve Points Won",       "first_serve_points_won"),
    ("2nd Serve Points Won",       "second_serve_points_won"),
    ("Break Points Saved",         "break_points_saved"),
    ("Service Games Played",       "service_games_played"),
    ("Return Rating",              "return_rating"),
    ("1st Serve Return Points Won","first_serve_return_points_won"),
    ("2nd Serve Return Points Won","second_serve_return_points_won"),
    ("Break Points Converted",     "break_points_converted"),
    ("Return Games Played",        "return_games_played"),
    ("Net Points Won",             "net_points_won"),
    ("Winners",                    "winners"),
    ("Unforced Errors",            "unforced_errors"),
    ("Service Points Won",         "service_points_won"),
    ("Return Points Won",          "return_points_won"),
    ("Total Points Won",           "total_points_won"),
    ("Max Speed",                  "max_speed"),
    ("1st Serve Average Speed",    "first_serve_avg_speed"),
    ("2nd Serve Average Speed",    "second_serve_avg_speed"),
] %}

with raw as (

    select
        data::json as data,
        meta_file_name,
        meta_file_modified
    from {{ source('infotennis_v2_raw', 'atp_key_stats') }}

),

base as (

    select
        json_extract(data, '$.data')                            as payload,
        json_extract_string(data, '$.metadata.retrieved_at')    as retrieved_at,
        meta_file_name,
        meta_file_modified
    from raw

),

sets as (

    -- Generate one row per possible set number (0 = match totals, 1-5 = sets)
    select
        b.*,
        s.set_number
    from base as b
    cross join (select unnest(generate_series(0, 5)) as set_number) as s
    where json_extract(b.payload, '$.setStats.set' || s.set_number) is not null

),

parsed as (

    select
        
        -- Match metadata
        cast(json_extract(payload, '$.setsCompleted') as int)   as sets_completed,
        set_number,

        -- Player 1 info
        json_extract_string(payload, '$.players[0].seed')               as player1_seed,
        json_extract_string(payload, '$.players[0].player1Name')        as player1_name,
        json_extract_string(payload, '$.players[0].player1FirstName')   as player1_first_name,
        json_extract_string(payload, '$.players[0].player1LastName')    as player1_last_name,
        json_extract_string(payload, '$.players[0].player1Id')          as player1_id,
        json_extract_string(payload, '$.players[0].player1Country')     as player1_country,

        -- Player 2 info
        json_extract_string(payload, '$.players[1].seed')               as player2_seed,
        json_extract_string(payload, '$.players[1].player1Name')        as player2_name,
        json_extract_string(payload, '$.players[1].player1FirstName')   as player2_first_name,
        json_extract_string(payload, '$.players[1].player1LastName')    as player2_last_name,
        json_extract_string(payload, '$.players[1].player1Id')          as player2_id,
        json_extract_string(payload, '$.players[1].player1Country')     as player2_country,

        -- The set-level stats array for this set_number
        json_extract(payload, '$.setStats.set' || set_number)           as set_stats_arr,

        -- File metadata
        meta_file_name,
        meta_file_modified,

    from sets

),

{#
    Helper: for each stat we find its element in the JSON array by scanning
    for the object where name = '<StatName>'.  DuckDB list_filter lets us
    do this without knowing the positional index.
#}

flattened as (

    select
        -- Match identification (parsed from S3 path)
        cast(regexp_extract(meta_file_name, 'year=(\d+)', 1) as int)    as year,
        regexp_extract(meta_file_name, 'tourn=([^/]+)', 1)              as tournament_id,
        regexp_extract(meta_file_name, '/([^/_]+)_\d{8}_\d{6}\.json', 1) as match_id,

        sets_completed,
        set_number,

        -- Players
        player1_seed,
        player1_name,
        player1_first_name,
        player1_last_name,
        player1_id,
        player1_country,
        player2_seed,
        player2_name,
        player2_first_name,
        player2_last_name,
        player2_id,
        player2_country,

        -- Stats (extracted by name matching)
        {% for raw_name, col_name in stats %}
        json_extract_string(
            list_filter(
                cast(set_stats_arr as json[]),
                x -> json_extract_string(x, '$.name') = '{{ raw_name }}'
            )[1],
            '$.player1'
        ) as {{ col_name }}_player1,
        json_extract_string(
            list_filter(
                cast(set_stats_arr as json[]),
                x -> json_extract_string(x, '$.name') = '{{ raw_name }}'
            )[1],
            '$.player2'
        ) as {{ col_name }}_player2{{ "," if not loop.last else "" }}
        {% endfor %},
        
        meta_file_name,
        meta_file_modified

    from parsed

)

select * from flattened
