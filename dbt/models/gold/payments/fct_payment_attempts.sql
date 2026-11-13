{{ config(
    materialized='incremental',
    unique_key='attempt_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    cluster_by=['attempt_date']
) }}

-- Grain: one row per payment attempt (latest lifecycle state).
-- Used by: finance (volume, fees, settlement), risk (declines, chargebacks),
-- collections (failed payments that become balances owed) and CX reporting.
select
    a.attempt_id,
    a.order_id,
    a.customer_id,                       -- masked unless role is PII-approved
    a.processor,                         -- which payment processor handled it
    a.status,                            -- authorized / captured / declined / refunded
    date(a.event_ts)                     as attempt_date,
    a.amount                             as amount_local,   -- customer's currency
    a.currency,
    round(a.amount * fx.usd_rate, 2)     as amount_usd,     -- one currency for global reporting
    s.settled_amount,                    -- cash received from processor
    s.fee_amount,                        -- processor cost for this transaction
    s.settlement_status,                 -- pending / settled / reversed (e.g. chargeback)
    a.event_ts                           as updated_at
from {{ ref('stg_payment_attempts') }} a
left join {{ ref('stg_fx_rates') }} fx
       on fx.currency = a.currency and fx.rate_date = date(a.event_ts)
left join {{ ref('stg_settlements') }} s
       on s.attempt_id = a.attempt_id
{% if is_incremental() %}
where a.event_ts > (select dateadd(hour, -6, max(updated_at)) from {{ this }})
{% endif %}
