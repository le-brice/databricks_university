-- int_project_payment_status: Classifies each project's payment health.
-- Consumed by mart_research_funding and mart_at_risk_projects.
-- Grain: one row per research project.

with projects as (
    select * from {{ ref('stg_research_projects') }}
),

payment_summary as (
    select
        project_id,
        count(*) as payment_count,
        sum(amount) as total_received,
        max(payment_date) as last_payment_date,
        sum(case when payment_type = 'final' then amount else 0 end)
            as final_payments_received
    from {{ ref('stg_grant_payments') }}
    group by project_id
),

enriched as (
    select
        p.project_id,
        p.status,
        p.grant_amount,
        p.start_date,
        p.end_date,
        p.funding_body,
        p.department_id,

        ps.last_payment_date,
        coalesce(ps.payment_count, 0) as payment_count,
        coalesce(ps.total_received, 0) as total_received,
        coalesce(ps.final_payments_received, 0) as final_payments_received,

        -- Disbursement rate: fraction of grant received so far
        round(
            coalesce(ps.total_received, 0) / nullif(p.grant_amount, 0),
            3
        ) as disbursement_rate,

        -- Time elapsed: fraction of project duration that has passed
        round(
            datediff(current_date(), p.start_date)
            / nullif(
                datediff(coalesce(p.end_date, current_date()), p.start_date),
                0
            ),
            3
        ) as pct_time_elapsed,

        -- Days since last payment (null if no payments ever received)
        datediff(current_date(), ps.last_payment_date)
            as days_since_last_payment

    from projects as p
    left join payment_summary as ps on p.project_id = ps.project_id
),

classified as (
    select
        *,

        -- Velocity: positive = ahead of pace; negative = behind schedule
        round(disbursement_rate - pct_time_elapsed, 3) as payment_velocity,

        case
            when status = 'terminated'
                then 'terminated'
            when status = 'pending'
                then 'not_started'
            when status = 'completed' and final_payments_received = 0
                then 'final_overdue'
            when status = 'completed'
                then 'closed'
            when
                status = 'active'
                and payment_count = 0
                and pct_time_elapsed > 0.10
                then 'no_payments_received'
            when
                status = 'active'
                and (disbursement_rate - pct_time_elapsed) < -0.20
                then 'at_risk'
            when
                status = 'active'
                and (disbursement_rate - pct_time_elapsed) > 0.10
                then 'ahead_of_schedule'
            else 'on_track'
        end as payment_status

    from enriched
)

select * from classified
