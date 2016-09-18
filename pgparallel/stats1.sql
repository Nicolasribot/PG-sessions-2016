-- launches series of queries with sample tables and different workers values

set max_parallel_workers_per_gather = 6;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample1 p
  join carreau_sample1 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 4;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample1 p
  join carreau_sample1 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 2;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample1 p
  join carreau_sample1 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 0;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample1 p
  join carreau_sample1 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 6;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample0 p
  join carreau_sample0 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 4;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample0 p
  join carreau_sample0 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 2;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample0 p
  join carreau_sample0 c on st_intersects(p.geom, c.geom);

set max_parallel_workers_per_gather = 0;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample0 p
  join carreau_sample0 c on st_intersects(p.geom, c.geom);


