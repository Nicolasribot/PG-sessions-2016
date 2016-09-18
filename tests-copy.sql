select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom);

-- ogr export: single threaded
-- ogr2ogr -sql "select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)" -f "ESRI Shapefile" inter.shp PG:"dbname=nicolas port=5439 user=test"


-- java, qgis, shell query: //

-- test copy:

set max_parallel_workers_per_gather = 0;

copy (
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
) TO '/tmp/q.copy';
-- DNW

-- test \copy
\copy (select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom) ) TO '/tmp/q.copy';
-- DNW

-- test copy with program:
create table tc (
  idparc bigint,
  idcarreau bigint,
  annee int,
  geom geometry
);

TRUNCATE tc;

copy tc FROM PROGRAM '/Users/nicolas/Projets/pg-session-2016/pgparallel/copy_par.bash'
with (DELIMITER '|');
-- yes: 81s !

-- direct access:

copy tc FROM PROGRAM $$/usr/local/pgsql-9.6/bin/psql \
  -A -t -p 5439 -U nicolas -d nicolas \
  -c "select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
      from parcelle_sample p
      join carreau_sample c on st_intersects(p.geom, c.geom)" $$
with (DELIMITER '|');

set max_parallel_workers_per_gather = 0;

drop table if exists inter_pgpar;
create table inter_pgpar (
  idparc bigint,
  idcarreau bigint,
  annee int,
  creation_time TIMESTAMP,
  geom geometry
) WITH (parallel_workers=6);

copy inter_pgpar FROM PROGRAM $$/usr/local/pgsql-9.6/bin/psql \
  -A -t -p 5439 -d nicolas \
  -c "set max_parallel_workers_per_gather = 6; select p.id as idparc, c.gid as idcarreau, p.annee,
        clock_timestamp() AS creation_time,
        st_intersection(p.geom, c.geom) as geom
      from parcelle_ssample p
      join carreau_ssample c on st_intersects(p.geom, c.geom)" $$
with (DELIMITER '|');
-- 24s

-- direct copy: works
-- psql96 -p 5439 -U test -d nicolas -c "select p.id as idparc, c.gid as idcarreau, p.annee,        clock_timestamp() AS creation_time,        st_intersection(p.geom, c.geom) as geom      from parcelle_ssample p      join carreau_ssample c on st_intersects(p.geom, c.geom)"|psql96 -p 5439 -c "copy inter_pgpar from STDIN"

-- fn copy with external psql: works, usable in SQL scripts.
