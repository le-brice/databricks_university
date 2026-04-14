-- ============================================================
-- TU/e Demo: raw.departments (15 rows, hardcoded)
-- ============================================================

CREATE OR REPLACE TABLE demo_tue.raw.departments AS
SELECT * FROM VALUES
  (1,  'Industrial Engineering & Innovation Sciences', 'Industrial Engineering & Innovation Sciences'),
  (2,  'Electrical Engineering',                       'Electrical Engineering'),
  (3,  'Mechanical Engineering',                       'Mechanical Engineering'),
  (4,  'Mathematics & Computer Science',               'Mathematics & Computer Science'),
  (5,  'Chemical Engineering & Chemistry',             'Chemical Engineering & Chemistry'),
  (6,  'Built Environment',                            'Built Environment'),
  (7,  'Applied Physics',                              'Applied Physics'),
  (8,  'Biomedical Engineering',                       'Biomedical Engineering'),
  (9,  'Industrial Design',                            'Industrial Design'),
  (10, 'Human Resources',                              'Support Services'),
  (11, 'Finance & Control',                            'Support Services'),
  (12, 'ICT Services',                                 'Support Services'),
  (13, 'Library & Information Services',               'Support Services'),
  (14, 'Facilities Management',                        'Support Services'),
  (15, 'Student Affairs',                              'Support Services')
AS t(department_id, department_name, faculty_name);
