# Project: databricks_university

A dbt demo project for university research finance use cases, built on Databricks.
Used to demo dbt to prospects in higher education / research sectors.

## Connections
- Warehouse: Databricks
- GitHub: https://github.com/le-brice/databricks_university (personal repo)

## dbt Cloud
- account-id: 587
- account-host: mj837.eu1.dbt.com
- project-id: 16897
- dev-environment-id: 59353
- prod-environment-id: 59397
- mcp-namespace: mcp__dbt-personal__*

## Data model
Source database: `demo_tue`, schema: `raw`

Two source domains:
- `raw_hr` — employees, departments
- `raw_finance` — research projects, grant payments

Staging models: stg_departments, stg_employees, stg_grant_payments, stg_research_projects
Mart models (finance): mart_grant_utilization, mart_research_funding

## Git
- Never commit to main
- Push to le-brice/databricks_university, open PRs from feature branches
- Gitmoji + conventional commits: `:emoji: type(scope): description`
