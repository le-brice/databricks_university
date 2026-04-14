-- ============================================================
-- TU/e Demo: raw.research_projects (300 rows)
-- principal_investigator_id: employees 1-220 (academic staff)
-- department_id: depts 1-9 (academic faculties)
-- grant_amount: log-normal skew — most €50K-500K, a few €1M-5M
-- ============================================================

CREATE OR REPLACE TABLE demo_tue.raw.research_projects AS
WITH base AS (
  SELECT
    id                                                                          AS project_id,
    concat(
      element_at(array(
        'Next-Generation','Sustainable','Smart','Autonomous','Bio-inspired',
        'High-Performance','Low-Carbon','Adaptive','Distributed','Quantum-Enabled'
      ), cast(floor(rand()*10)+1 AS INT)),
      ' ',
      element_at(array(
        'Energy Storage Systems','Semiconductor Devices','Healthcare Monitoring',
        'Urban Mobility Networks','Water Treatment Processes','Machine Learning Pipelines',
        'Photovoltaic Materials','Wireless Communication','Supply Chain Resilience',
        'Robotic Surgery Platforms','Building Automation','Drug Delivery Systems',
        'Neural Interface Technology','Battery Management Systems','Circular Manufacturing',
        'Climate-Resilient Infrastructure','Personalized Medicine','Edge Computing',
        'Structural Health Monitoring','Precision Agriculture'
      ), cast(floor(rand()*20)+1 AS INT))
    )                                                                           AS title,
    -- PI from academic staff 1-220
    cast(floor(rand() * 220) + 1 AS INT)                                       AS principal_investigator_id,
    -- Academic departments 1-9
    (id % 9) + 1                                                               AS department_id,
    element_at(array(
      'NWO','NWO','NWO',
      'Horizon Europe','Horizon Europe',
      'ERC',
      'ASML Research','Philips Research','NXP Semiconductors',
      'Brainport Development','Eindhoven Engine',
      'ZonMW','KNAW','STW'
    ), cast(floor(rand()*14)+1 AS INT))                                        AS funding_body,
    -- Grant amount: combination of a base tier + random within tier
    CASE
      WHEN rand() < 0.05 THEN cast(floor(rand() * 4000000 + 1000000) AS BIGINT)  -- 5%: €1M-5M (large ERC/Horizon)
      WHEN rand() < 0.25 THEN cast(floor(rand() * 500000  + 300000)  AS BIGINT)  -- 20%: €300K-800K
      ELSE                     cast(floor(rand() * 250000  + 50000)   AS BIGINT)  -- 75%: €50K-300K (NWO/industry)
    END                                                                         AS grant_amount,
    date_add(to_date('2019-01-01'), cast(floor(rand() * 1461) AS INT))         AS start_date,
    rand()                                                                      AS _duration_r,
    element_at(array('active','active','active','completed','completed','pending','terminated'),
      cast(floor(rand()*7)+1 AS INT))                                          AS status,
    element_at(array(
      'AI & Data Science','AI & Data Science',
      'Semiconductor Technology','Semiconductor Technology',
      'Energy Transition','Energy Transition',
      'Smart Mobility',
      'Health Technology','Health Technology',
      'Quantum Computing',
      'Circular Economy',
      'Photonics',
      'Sustainable Architecture',
      'Bio-inspired Engineering'
    ), cast(floor(rand()*14)+1 AS INT))                                        AS research_theme
  FROM (SELECT explode(sequence(1, 300)) AS id)
),
with_end AS (
  SELECT
    project_id,
    title,
    principal_investigator_id,
    department_id,
    funding_body,
    grant_amount,
    start_date,
    date_add(start_date, cast(_duration_r * 1460 + 365 AS INT))               AS end_date,
    status,
    research_theme
  FROM base
)
SELECT
  project_id,
  title,
  principal_investigator_id,
  department_id,
  funding_body,
  grant_amount,
  start_date,
  -- end_date should not be in far future for completed projects
  CASE
    WHEN status = 'completed' AND end_date > current_date()
      THEN date_add(current_date(), -cast(floor(rand()*365) AS INT))
    ELSE end_date
  END AS end_date,
  status,
  research_theme
FROM with_end;
