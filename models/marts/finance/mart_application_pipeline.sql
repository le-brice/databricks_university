{{ config(
    materialized = 'table',
    tags         = ['finance', 'grants', 'daily']
) }}

-- mart_application_pipeline: Grant application funnel with acceptance rates.
-- Grain: one row per application.

with applications as (
    select * from {{ ref('grant_applications') }}
),

enriched as (
    select
        application_id,
        research_theme,
        funding_body,
        department_name,
        submission_date,
        outcome,
        grant_amount_requested,
        review_duration_days,

        -- Derived: funder category
        case
            when funding_body in ('NWO')                     then 'Public'
            when funding_body in ('ERC', 'Horizon Europe')   then 'EU'
            else                                                  'Industry'
        end                                                         as funder_type,

        -- Derived: boolean outcome flags
        outcome = 'awarded'                                         as is_awarded,
        outcome = 'rejected'                                        as is_rejected,
        outcome = 'pending'                                         as is_pending,

        -- Derived: submission year
        year(to_date(submission_date))                              as submission_year,

        -- Derived: academic year of submission
        {{ academic_year('to_date(submission_date)') }}             as submission_academic_year,

        -- Derived: review speed bucket
        case
            when review_duration_days <= 60  then 'fast'
            when review_duration_days <= 150 then 'standard'
            else                                  'slow'
        end                                                         as review_speed,

        -- Pipeline metadata
        current_timestamp()                                         as _loaded_at

    from applications
),

-- Acceptance rates via window functions — no separate agg model needed
with_rates as (
    select
        *,

        -- Funding body acceptance rate
        round(
            sum(case when is_awarded then 1 else 0 end) over (partition by funding_body)
            / nullif(count(*) over (partition by funding_body), 0) * 100
        , 1)                                                        as funding_body_acceptance_rate,

        -- Department acceptance rate
        round(
            sum(case when is_awarded then 1 else 0 end) over (partition by department_name)
            / nullif(count(*) over (partition by department_name), 0) * 100
        , 1)                                                        as dept_acceptance_rate,

        -- Research theme acceptance rate
        round(
            sum(case when is_awarded then 1 else 0 end) over (partition by research_theme)
            / nullif(count(*) over (partition by research_theme), 0) * 100
        , 1)                                                        as theme_acceptance_rate,

        -- Total applications per funding body (useful for BI context)
        count(*) over (partition by funding_body)                   as funding_body_total_applications,
        count(*) over (partition by department_name)                as dept_total_applications

    from enriched
)

select * from with_rates
