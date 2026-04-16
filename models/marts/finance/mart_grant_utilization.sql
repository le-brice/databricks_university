-- mart_grant_utilization: Grant utilization by department and academic year.
-- Grain: one row per department per academic year.

with funding as (
    select * from {{ ref('mart_research_funding') }}
),

dept_year_summary as (
    select
        department_name,
        faculty_name,
        start_academic_year                                                 as academic_year,

        -- Portfolio size
        count(*)                                                            as total_projects,
        sum(case when status = 'active'     then 1 else 0 end)             as active_projects,
        sum(case when status = 'completed'  then 1 else 0 end)             as completed_projects,
        sum(case when status = 'pending'    then 1 else 0 end)             as pending_projects,

        -- Grant value
        sum(grant_amount)                                                   as total_grant_value,
        avg(grant_amount)                                                   as avg_grant_size,
        max(grant_amount)                                                   as largest_grant,

        -- Payments received
        sum(total_payments_received)                                        as total_received,

        -- Utilization rate
        round(
            sum(total_payments_received) / nullif(sum(grant_amount), 0),
        3)                                                                  as utilization_rate,

        -- Remaining
        sum(remaining_grant)                                                as total_remaining,

        -- Funding body breakdown
        sum(case when funding_body = 'NWO'             then grant_amount else 0 end) as nwo_grant_total,
        sum(case when funding_body = 'ERC'             then grant_amount else 0 end) as erc_grant_total,
        sum(case when funding_body = 'Horizon Europe'  then grant_amount else 0 end) as horizon_grant_total,
        sum(case when funding_body in (
            'ASML Research','Philips Research','NXP Semiconductors',
            'Brainport Development','Eindhoven Engine'
        ) then grant_amount else 0 end)                                     as industry_grant_total,

        -- Research theme breakdown
        count(distinct research_theme)                                      as distinct_research_themes,

        -- Project counts per funding body
        sum(case when funding_body = 'NWO'            then 1 else 0 end)   as nwo_project_count,
        sum(case when funding_body = 'ERC'            then 1 else 0 end)   as erc_project_count,
        sum(case when funding_body = 'Horizon Europe' then 1 else 0 end)   as horizon_project_count,

        -- Pipeline metadata
        current_timestamp()                                                 as _loaded_at

    from funding
    group by department_name, faculty_name, start_academic_year
)

select * from dept_year_summary
