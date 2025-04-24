SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM {{ source('raw', 'stock_data') }}
