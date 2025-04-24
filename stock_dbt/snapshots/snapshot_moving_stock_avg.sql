{% snapshot snapshot_stock_moving_avg %}

{{
  config(
    target_schema='snapshot',
    unique_key="symbol || '-' || to_varchar(date)",
    strategy='check',
    check_cols=['moving_avg_7d'],
    invalidate_hard_deletes=True
  )
}}

SELECT 
  symbol,
  date,
  moving_avg_7d
FROM {{ ref('stock_moving_avg') }}

{% endsnapshot %}
