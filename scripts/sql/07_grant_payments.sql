-- ============================================================
-- TU/e Demo: raw.grant_payments (800 rows)
-- Payment dates fall within project start/end window
-- payment_type reflects real grant disbursement patterns
-- ============================================================

CREATE OR REPLACE TABLE demo_tue.raw.grant_payments AS
WITH projects AS (
  SELECT project_id, grant_amount, start_date, end_date
  FROM demo_tue.raw.research_projects
),
numbered_projects AS (
  SELECT *, row_number() OVER (ORDER BY project_id) AS rn
  FROM projects
),
base AS (
  SELECT
    id                                                                       AS payment_id,
    -- Each project gets ~2-3 payments; cycle through all 300 projects
    ((id - 1) % 300) + 1                                                     AS project_id,
    element_at(array('advance','advance','milestone','milestone','milestone','final','overhead'),
      cast(floor(rand()*7)+1 AS INT))                                       AS payment_type,
    rand()                                                                   AS _date_frac
  FROM (SELECT explode(sequence(1, 800)) AS id)
),
joined AS (
  SELECT
    b.payment_id,
    b.project_id,
    b.payment_type,
    -- Payment date within project window
    date_add(
      p.start_date,
      cast(b._date_frac * datediff(coalesce(p.end_date, date_add(p.start_date, 730)), p.start_date) AS INT)
    )                                                                        AS payment_date,
    -- Amount: proportion of grant based on payment type
    CASE b.payment_type
      WHEN 'advance'   THEN cast(p.grant_amount * (rand() * 0.15 + 0.10) AS BIGINT)
      WHEN 'milestone' THEN cast(p.grant_amount * (rand() * 0.20 + 0.15) AS BIGINT)
      WHEN 'final'     THEN cast(p.grant_amount * (rand() * 0.20 + 0.20) AS BIGINT)
      WHEN 'overhead'  THEN cast(p.grant_amount * (rand() * 0.05 + 0.02) AS BIGINT)
      ELSE                  cast(p.grant_amount * 0.10 AS BIGINT)
    END                                                                      AS amount
  FROM base b
  JOIN numbered_projects p ON b.project_id = p.project_id
)
SELECT payment_id, project_id, payment_date, amount, payment_type
FROM joined;
