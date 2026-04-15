SELECT
  research_theme,
  COUNT(*) AS project_count,
  SUM(grant_amount) AS total_funding_eur,
  ROUND(AVG(grant_amount),2) AS avg_grant_eur
FROM {{ ref('mart_research_funding') }}
GROUP BY research_theme
ORDER BY total_funding_eur DESC