-- mart_research_funding: Research project portfolio with funding progress.
-- Grain: one row per research project.

with projects as (
    select * from {{ ref('stg_research_projects') }}
),

departments as (
    select * from {{ ref('stg_departments') }}
),

employees as (
    select employee_id, full_name, role from {{ ref('stg_employees') }}
),

-- Aggregate payments per project
payment_summary as (
    select
        project_id,
        count(*)                                                            as payment_count,
        sum(amount)                                                         as total_payments_received,
        min(payment_date)                                                   as first_payment_date,
        max(payment_date)                                                   as last_payment_date,
        sum(case when payment_type = 'advance'   then amount else 0 end)   as advance_payments,
        sum(case when payment_type = 'milestone' then amount else 0 end)   as milestone_payments,
        sum(case when payment_type = 'final'     then amount else 0 end)   as final_payments,
        sum(case when payment_type = 'overhead'  then amount else 0 end)   as overhead_payments
    from {{ ref('stg_grant_payments') }}
    group by project_id
),

joined as (
    select
        p.project_id,
        p.title,
        p.status,
        p.funding_body,
        p.research_theme,
        p.grant_amount,
        p.start_date,
        p.end_date,

        -- Department & PI details
        d.department_name,
        d.faculty_name,
        e.full_name                                                         as principal_investigator,
        e.role                                                              as pi_role,

        -- Academic year the project started
        {{ academic_year('p.start_date') }}                                 as start_academic_year,

        -- Duration
        datediff(
            coalesce(p.end_date, current_date()),
            p.start_date
        )                                                                   as project_duration_days,

        round(
            datediff(coalesce(p.end_date, current_date()), p.start_date) / 365.25,
            1
        )                                                                   as project_duration_years,

        -- Funding intensity (€ per year)
        round(p.grant_amount / nullif(
            datediff(coalesce(p.end_date, current_date()), p.start_date) / 365.25,
        0), 0)                                                              as annual_funding_intensity,

        -- Payment progress
        coalesce(ps.payment_count, 0)                                       as payment_count,
        coalesce(ps.total_payments_received, 0)                             as total_payments_received,
        coalesce(ps.advance_payments, 0)                                    as advance_payments,
        coalesce(ps.milestone_payments, 0)                                  as milestone_payments,
        coalesce(ps.final_payments, 0)                                      as final_payments,
        coalesce(ps.overhead_payments, 0)                                   as overhead_payments,
        ps.first_payment_date,
        ps.last_payment_date,

        -- Utilization: payments received as % of grant
        round(
            coalesce(ps.total_payments_received, 0) / nullif(p.grant_amount, 0) * 100,
        1)                                                                  as pct_grant_received,

        -- Remaining to be disbursed
        p.grant_amount - coalesce(ps.total_payments_received, 0)           as remaining_grant,

        -- Flag: project receiving final payment or complete
        ps.final_payments > 0                                               as has_final_payment,

        -- Pipeline metadata
        current_timestamp()                                                 as _loaded_at

    from projects p
    left join departments d using (department_id)
    left join employees   e on p.principal_investigator_id = e.employee_id
    left join payment_summary ps using (project_id)
)

select * from joined
