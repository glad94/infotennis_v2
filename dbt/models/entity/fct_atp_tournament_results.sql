with staging as (

    -- Deduping is already handled accurately in the underlying staging view
    select *
    from {{ ref('stg_atp_tournament_results') }}

),

final as (

    select
        -- Identifiers
        year,
        tournament_id,
        tournament_name,
        upper(match_id) as match_id,

        -- Match Context
        round,
        score,
        url,

        -- Player 1
        player1_id,
        player1_name,
        player1_seed,
        player1_nation,

        -- Player 2
        player2_id,
        player2_name,
        player2_seed,
        player2_nation

    from staging

)

select * from final
