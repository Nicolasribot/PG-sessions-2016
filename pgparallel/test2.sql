-- Tests // with several workers settings
set max_parallel_workers_per_gather = 0;
set max_parallel_workers_per_gather = 1;
set max_parallel_workers_per_gather = 2;
set max_parallel_workers_per_gather = 4;
set max_parallel_workers_per_gather = 6;
set max_parallel_workers_per_gather = 8;

explain analyse SELECT
  p.id AS idparc,
  c.gid AS idcarreau,
  p.annee,
--   st_multi(st_intersection(p.geom, c.geom)) :: GEOMETRY(MULTIPOLYGON, 2154) AS geom,
  st_intersection(p.geom, c.geom)AS geom,
  clock_timestamp() AS creation_time
FROM parcelle_ssample p
  JOIN carreau_ssample c ON st_intersects(p.geom, c.geom);

-- 0: No //, Execution time: 80950.223 ms
-- 1: No //: 81
-- 2: //: 27868.677 ms planned: 2w
-- 4: //: 21833.858 ms planned: 3w
-- 6: //: 22030.729 ms planned: 3w
-- 8: //: 21634.833 ms planned: 3w
