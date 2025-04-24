SELECT
    symbol,
    date,
    actual,
    forecast,
    lower_bound,
    upper_bound
FROM {{ source('analytics', 'stock_data') }}
