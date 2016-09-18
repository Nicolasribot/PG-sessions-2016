-- stocker dans des stats:
create table result (
  id SERIAL PRIMARY KEY ,
  method text,
  query text,
  exec_time int,
  cpu int,
  cpu_planned int
);

insert into result(method, query, exec_time, cpu, cpu_planned) VALUES
  ('pgpar', 'sample2', 81, 1, 1),
  ('pgpar', 'sample2', 28, 2, 2),
  ('pgpar', 'sample2', 22, 4, 3),
  ('pgpar', 'sample2', 22, 6, 3);

-- sample tables:
-- parcelle: full dataset
-- sample1: 10% random
-- sample2: HG extent
-- sample3: small HG extent.

-- some stats query
with tmp as (
  select *
  from result
  where cpu = 1
) select r.method, r.exec_time, r.query, r.cpu, r.cpu_planned,
    round(r.exec_time::NUMERIC/t.exec_time::NUMERIC, 3) * 100 as exec_ratio,
    (1 - round(r.exec_time::NUMERIC/t.exec_time::NUMERIC, 3)) * 100 as gain_ratio
from result r join tmp t on r.method = t.method and r.query = t.query;


-- FMI: sample2:
FMI Execution time: (split: 2, jobs: 2): 38 s.
FMI Execution time: (split: 2, jobs: 4): 24 s.
FMI Execution time: (split: 4, jobs: 2): 42 s.
FMI Execution time: (split: 4, jobs: 4): 27 s.
Total Execution time: 131 s.

FMI Execution time: (split: 4, jobs: 4): 25 s.
FMI Execution time: (split: 4, jobs: 6): 24 s.
FMI Execution time: (split: 6, jobs: 4): 27 s.
FMI Execution time: (split: 6, jobs: 6): 25 s.
FMI Execution time: (split: 4, jobs: 8): 22 s.
FMI Execution time: (split: 6, jobs: 8): 25 s.
FMI Execution time: (split: 8, jobs: 8): 26 s.
Total Execution time: 101 s.

-- sample1
PG // Execution time (8 workers): 107 s.
Par-psql Execution time: 103 s.
FMI Execution time: (split: 5, jobs: 8): 206 s.
Total Execution time: 416 s.

-- sample0:
PG // Execution time (8 workers): 748 s.
Par-psql Execution time: 720 s.
FMI Execution time: (split: 8, jobs: 8): 1829 s.
FMI Execution time: (split: 12, jobs: 8): 2403 s
Total Execution time: 3299 s.