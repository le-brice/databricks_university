-- ============================================================
-- TU/e Demo: raw.employees (500 rows)
-- Employees 1-300: academic staff (depts 1-9)
-- Employees 301-500: support staff (depts 10-15)
-- ~15% have a termination_date (departed employees)
-- ============================================================

CREATE OR REPLACE TABLE demo_tue.raw.employees AS
WITH base AS (
  SELECT
    id                                                                      AS employee_id,
    element_at(array(
      'Jan','Pieter','Willem','Hendrik','Thomas','Ruben','Jeroen','Lars','Sven','Bas',
      'Koen','Tim','Robin','Mark','Erik','Maarten','Joost','Niels','Daan','Luuk',
      'Anna','Emma','Lisa','Sophie','Julia','Maria','Laura','Sanne','Inge','Nora',
      'Fleur','Rosa','Lotte','Eva','Hana','Ahmed','Omar','Wei','Fatima','Priya'
    ), cast(floor(rand() * 40) + 1 AS INT))                                AS first_name,
    element_at(array(
      'de Vries','Bakker','Janssen','Visser','Smit','Meijer','de Boer','Peters','Mulder',
      'van den Berg','van Dijk','Bos','Hendriks','Kok','Vermeulen','van Leeuwen','Linders',
      'Willems','Kuijpers','Jacobs','Brouwers','Verhoeven','Peeters','Claes','Hermans',
      'Dubois','Nguyen','Al-Hassan','Fernandez','Kumar'
    ), cast(floor(rand() * 30) + 1 AS INT))                                AS last_name,
    -- Academic depts 1-9 for first 300, support depts 10-15 for 301-500
    CASE
      WHEN id <= 300 THEN (id % 9) + 1
      ELSE (id % 6) + 10
    END                                                                     AS department_id,
    CASE
      WHEN id <= 50  THEN 'professor'
      WHEN id <= 130 THEN 'associate_professor'
      WHEN id <= 220 THEN 'assistant_professor'
      WHEN id <= 300 THEN element_at(array('postdoc','phd_candidate','lecturer','researcher'), cast(floor(rand()*4)+1 AS INT))
      WHEN id <= 380 THEN element_at(array('admin_staff','technician','it_specialist'), cast(floor(rand()*3)+1 AS INT))
      ELSE element_at(array('admin_staff','financial_analyst','hr_officer','facility_manager'), cast(floor(rand()*4)+1 AS INT))
    END                                                                     AS role,
    date_add(to_date('2005-01-01'), cast(floor(rand() * 6935) AS INT))     AS hire_date,
    rand()                                                                  AS _term_chance,
    cast(floor(rand() * 1460 + 365) AS INT)                                AS _days_to_term,
    element_at(array('full_time','full_time','full_time','part_time','part_time','temporary','visiting'),
      cast(floor(rand() * 7) + 1 AS INT))                                  AS employment_type
  FROM (SELECT explode(sequence(1, 500)) AS id)
)
SELECT
  employee_id,
  first_name,
  last_name,
  department_id,
  role,
  hire_date,
  CASE WHEN _term_chance < 0.15
       THEN date_add(hire_date, _days_to_term)
       ELSE NULL
  END                                                                       AS termination_date,
  employment_type
FROM base;
