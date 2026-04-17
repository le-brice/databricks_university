SELECT
  research_theme,
  COUNT(*) AS total_applications,
  SUM(CASE WHEN is_awarded THEN 1 ELSE 0 END) AS awarded_applications,
  ROUND( SUM(CASE WHEN is_awarded THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) * 100, 1) AS acceptance_rate_pct
FROM {{ ref('mart_application_pipeline') }}
GROUP BY research_theme