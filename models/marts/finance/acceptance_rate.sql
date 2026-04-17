SELECT 
    funding_body,
    ROUND(SUM(CASE WHEN outcome = 'awarded' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS acceptance_rate
FROM 
    {{ ref('mart_application_pipeline') }}
WHERE 
    funding_body IN ('NWO', 'ERC', 'Horizon Europe', 'ASML Research', 'Philips Research', 'NXP Semiconductors', 'Brainport Development', 'Eindhoven Engine')
GROUP BY 
    funding_body
ORDER BY 
    acceptance_rate DESC