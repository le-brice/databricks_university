SELECT 
    funding_body,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS acceptance_rate
FROM 
    {{ ref('mart_research_funding') }}
WHERE 
    funding_body IN ('NWO', 'ERC', 'Horizon Europe', 'ASML Research', 'Philips Research', 'NXP Semiconductors', 'Brainport Development', 'Eindhoven Engine')
GROUP BY 
    funding_body