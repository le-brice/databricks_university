-- mart_at_risk_projects: Active projects with concerning payment patterns.
-- Ordered by urgency score — largest outstanding value against tightest deadlines first.
-- Used by the Finance & Research Office to prioritise funder follow-up.
-- Grain: one row per at-risk project.

with at_risk as (
    select * from {{ ref('int_project_payment_status') }}
    where payment_status in ('at_risk', 'no_payments_received', 'final_overdue')
),

projects as (
    select
        project_id,
        title,
        principal_investigator_id,
        research_theme
    from {{ ref('stg_research_projects') }}
),

departments as (
    select department_id, department_name, faculty_name
    from {{ ref('stg_departments') }}
),

employees as (
    select employee_id, full_name, role
    from {{ ref('stg_employees') }}
),

joined as (
    select
        ar.project_id,
        p.title,
        p.research_theme,

        -- Risk classification
        ar.payment_status                                               as risk_status,
        ar.funding_body,

        -- Payment health metrics
        ar.payment_velocity,
        round(ar.pct_time_elapsed * 100, 1)                            as pct_time_elapsed,
        round(ar.disbursement_rate * 100, 1)                           as pct_grant_disbursed,

        -- Grant financials
        ar.grant_amount,
        ar.total_received,
        ar.grant_amount - ar.total_received                            as outstanding_amount,
        ar.last_payment_date,
        ar.days_since_last_payment,

        -- Timeline pressure
        ar.start_date,
        ar.end_date,
        datediff(ar.end_date, current_date())                          as days_to_deadline,

        -- Urgency score: outstanding value weighted by how far into the project we are.
        -- Higher = needs attention soonest.
        round(
            (ar.grant_amount - ar.total_received)
            * greatest(coalesce(ar.pct_time_elapsed, 0), 0.1),
        0)                                                             as urgency_score,

        -- Responsible contacts
        d.department_name,
        d.faculty_name,
        e.full_name                                                    as principal_investigator,
        e.role                                                         as pi_role

    from at_risk ar
    join projects p         using (project_id)
    left join departments d on ar.department_id = d.department_id
    left join employees e   on p.principal_investigator_id = e.employee_id
)

select * from joined
order by urgency_score desc
