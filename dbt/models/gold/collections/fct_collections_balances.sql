-- Collections data product, built on the same payments platform.
-- When payments fail and retries are exhausted, the amount becomes a balance
-- owed (e.g. unpaid seller fees). Balances are aged and, past a threshold,
-- placed with external collection agencies. Agencies send back recovery files
-- through the same contract-validated ingestion pattern shown above.
{{ config(materialized='incremental', unique_key='account_id', on_schema_change='fail') }}

with failed_payments as (
    select
        customer_id                          as account_id,
        sum(amount_usd)                      as amount_owed_usd,
        min(attempt_date)                    as first_failed_date
    from {{ ref('fct_payment_attempts') }}
    where status = 'declined'
      and attempt_date < dateadd(day, -7, current_date)  -- 7-day automatic retry window has passed
    group by 1
),

recoveries as (
    select
        account_id,
        sum(recovered_amount_usd)            as recovered_usd,
        max(agency_name)                     as placed_with_agency
    from {{ ref('stg_agency_recovery_files') }}   -- external agency feeds
    group by 1
)

select
    f.account_id,
    f.amount_owed_usd,
    coalesce(r.recovered_usd, 0)                             as recovered_usd,
    f.amount_owed_usd - coalesce(r.recovered_usd, 0)         as outstanding_usd,
    datediff(day, f.first_failed_date, current_date)         as days_delinquent,
    case                                                     -- aging bucket for collections strategy
        when datediff(day, f.first_failed_date, current_date) <= 30 then '0-30'
        when datediff(day, f.first_failed_date, current_date) <= 60 then '31-60'
        when datediff(day, f.first_failed_date, current_date) <= 90 then '61-90'
        else '90+'
    end                                                      as aging_bucket,
    r.placed_with_agency,
    div0(coalesce(r.recovered_usd, 0), f.amount_owed_usd)     as recovery_rate
from failed_payments f
left join recoveries r using (account_id)
