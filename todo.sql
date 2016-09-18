-- Parallel queries and big data:
-- Traitements spatiaux parallélisés pour les gros volumes de données
-- 20 min

-- parsql= https://github.com/gbb/par_psql

-- FMI: fast map intersection: https://github.com/gbb/fast_map_intersection
-- decoupe d'une requete en petites parties lancées en //
-- script shell à editer pour matcher sa requete.
-- cas d'utilisation: intersection de deux couches PG
--

-- 9.6 parallel queries:
-- TODO: install latest version: tests on it

-- PLAN:
-- ce qu'on va dire
-- ce qu'on ne va PAS dire: sujet trop vaste
--
-- TODO: config machine, PG, disques
-- TODO: // spatial aggreg

-----------------------------------------------
-- Data used:
-- parcelle table: couverture multi annee sur la zone.
alter table ref.parcelle set schema public;

-- nettoyage: on ne garde que 3 cols
alter table parcelle drop column id_source;

create index parcelle_geom_gist on parcelle USING GIST (geom);
-- creation index spatial: completed in 30m 1s 141ms

VACUUM ANALYSE parcelle;
-- vacuum time: Time: 136.671 s

-- stats
-- table size
select pg_size_pretty(pg_table_size('parcelle'));
-- 28 GB

select pg_size_pretty(pg_total_relation_size('parcelle'));
-- total size: 40 GB

select pg_size_pretty(pg_indexes_size('parcelle'));
-- => index size: 12 GB

select count(*) from parcelle;
-- 97 998 398

select sum(st_npoints(geom)), min(st_npoints(geom)), max(st_npoints(geom)), avg(st_npoints(geom)) from parcelle;
-- num coords:
--     sum        | min |  max  |         avg
-- ---------------+-----+-------+---------------------
--  1 245 064 747 |   4 | 11539 | 12.8
-- (1 row)
--
-- Time: 123461.226 ms

-- annees:
select distinct annee from parcelle;
-- 5
-- 7
-- 2010
-- 2015

select annee, count(*)
from parcelle
GROUP BY annee;
--  annee |  count
-- -------+----------
--      5 |  4 584 414
--      7 |    188 088
--   2010 | 89 286 583
--   2015 |  3 939 313
-- (4 rows)
--
-- Time: 10279.536 ms


-- autre couche:
-- carroyage insee: 2 278 213
-- /usr/local/pgsql-9.6/bin/shp2pgsql -IiD -g geom -s 27572 /Users/nicolas/tmp/Archive/200m-carreaux-metropole.shp carreau | psql96
select distinct st_numgeometries(geom)
  from carreau;
-- forcage PG simple
alter table carreau ALTER COLUMN geom type geometry (POLYGON, 2154) using st_transform(st_geometryN(st_setSRID(geom, 27572), 1), 2154);

VACUUM ANALYSE carreau;
-- stats
-- table size
select pg_size_pretty(pg_table_size('carreau'));
-- 445 MB

select pg_size_pretty(pg_total_relation_size('carreau'));
-- total size: 623 MB

select pg_size_pretty(pg_indexes_size('carreau'));
-- => index size: 178 MB

select count(*) from carreau;
-- 2 278 213

select sum(st_npoints(geom)), min(st_npoints(geom)), max(st_npoints(geom)), avg(st_npoints(geom))
from carreau;
-- num coords:
-- 11 391 065	5	5	5


-------------------------------------------------------
-- preparation sample table to test queries:
drop table if exists parcelle_sample;
create table parcelle_sample as select * from parcelle TABLESAMPLE SYSTEM (1);
-- 4s....@1%, 1min8s @10%
select count(*) from parcelle_sample;
-- 980419 @1%, 9749601 @10%

alter table parcelle_sample add PRIMARY KEY (id);
create index parcelle_sample_geom_gist on parcelle_sample USING gist(geom);
VACUUM ANALYSE parcelle_sample;

drop table if exists carreau_sample;
create table carreau_sample as select * from carreau TABLESAMPLE SYSTEM (1);
select count(*) from carreau_sample;
-- 22840

alter table carreau_sample add PRIMARY KEY (id);
create index carreau_sample_geom_gist on carreau_sample USING gist(geom);
VACUUM ANALYSE carreau_sample;

-- test on sample:
set max_parallel_workers_per_gather = 0;

explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom);
-- Nested Loop  (cost=0.42..114032310.60 rows=206993640 width=44) (actual time=0.349..112857.333 rows=749426 loops=1)
--   ->  Seq Scan on carreau_sample c  (cost=0.00..8009.40 rows=228840 width=124) (actual time=0.008..57.055 rows=228840 loops=1)
--   ->  Index Scan using parcelle_sample_geom_gist on parcelle_sample p  (cost=0.42..268.89 rows=325 width=262) (actual time=0.092..0.135 rows=3 loops=228840)
--         Index Cond: (geom && c.geom)
--         Filter: _st_intersects(geom, c.geom)
--         Rows Removed by Filter: 1
-- Planning time: 3.886 ms
-- Execution time: 112944.536 ms

-- with 4 workers:
-- Gather  (cost=1000.28..88729346.47 rows=206993640 width=44) (actual time=2.447..83035.073 rows=749426 loops=1)
--   Workers Planned: 4
--   Workers Launched: 4
--   ->  Nested Loop  (cost=0.28..68028982.47 rows=206993640 width=44) (actual time=1.325..82904.390 rows=149885 loops=5)
--         ->  Parallel Seq Scan on parcelle_sample p  (cost=0.00..394063.19 rows=2437319 width=262) (actual time=0.017..562.482 rows=1949920 loops=5)
--         ->  Index Scan using carreau_sample_geom_gist on carreau_sample c  (cost=0.28..6.44 rows=8 width=124) (actual time=0.030..0.031 rows=0 loops=9749601)
--               Index Cond: (p.geom && geom)
--               Filter: _st_intersects(p.geom, geom)
--               Rows Removed by Filter: 0
-- Planning time: 3.718 ms
-- Execution time: 83155.389 ms

-- with 8 workers:
-- Gather  (cost=1000.28..83425716.81 rows=206993640 width=44) (actual time=2.965..77794.131 rows=749426 loops=1)
--   Workers Planned: 6
--   Workers Launched: 6
--   ->  Nested Loop  (cost=0.28..62725352.81 rows=206993640 width=44) (actual time=1.336..77649.822 rows=107061 loops=7)
--         ->  Parallel Seq Scan on parcelle_sample p  (cost=0.00..385938.79 rows=1624879 width=262) (actual time=0.023..498.988 rows=1392800 loops=7)
--         ->  Index Scan using carreau_sample_geom_gist on carreau_sample c  (cost=0.28..6.44 rows=8 width=124) (actual time=0.039..0.040 rows=0 loops=9749601)
--               Index Cond: (p.geom && geom)
--               Filter: _st_intersects(p.geom, geom)
--               Rows Removed by Filter: 0
-- Planning time: 3.901 ms
-- Execution time: 77854.347 ms


-- time: 1.8s @1%, s @10%

set max_parallel_workers_per_gather = 8;
set max_parallel_workers_per_gather = 0;

drop table if exists inter_sample2;
create table inter_sample2 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom);
-- time: completed in 2m 7s 264ms

select count(*)
from inter_sample2;

select count(*)
from inter_sample;
-- 749426

drop TABLE inter_sample;

-- FMI:
-- original query to edit:
select p.id, c.gid, st_intersection(p.geom, c.geom) as geom
from parcelle p
  join carreau c on st_intersects(p.geom, c.geom)
where p.annee = 2015;

-- Nested Loop  (cost=72453.13..3927327567.68 rows=2941935131 width=62)
--   ->  Bitmap Heap Scan on parcelle p  (cost=72444.10..3980064.83 rows=3874004 width=261)
--         Recheck Cond: (annee = 2015)
--         ->  Bitmap Index Scan on parcelle_annee_idx  (cost=0.00..71475.60 rows=3874004 width=0)
--               Index Cond: (annee = 2015)
--   ->  Bitmap Heap Scan on carreau c  (cost=9.03..822.13 rows=76 width=54)
--         Recheck Cond: (p.geom && geom)
--         Filter: _st_intersects(p.geom, geom)
--         ->  Bitmap Index Scan on carreau_geom_idx  (cost=0.00..9.01 rows=228 width=0)
--               Index Cond: (p.geom && geom)

-- Test 1: query:
-- select p.id, c.gid, st_intersection(p.geom, c.geom) as geom
-- from parcelle p
--   join carreau c on st_intersects(p.geom, c.geom)
-- where p.annee = 2015
--   AND p.id%$3=${1}
--   AND c.gid%$3=${2};
--
-- config:
-- WORK_MEM=200
-- SPLIT=10
-- JOBS=4

-- res:
-- 8 cpu detected by script (hyperthreading ?)
-- relaunch with 8 cpus

------------------------
-- parallel in PG:
-- params for parallel
-- depesz blog on // to test feature on PG6:
create table test as
    select
        i as id,
        now() - random() * '5 years'::interval as ts,
        (random() * 100000000)::int4 as number,
        repeat('x', (10 + 40 * random())::int4) as txt
    from
        generate_series(1, 10000000) i;

alter table test add primary key (id);

explain analyze select min(ts) from test;
-- Aggregate  (cost=226492.00..226492.01 rows=1 width=8) (actual time=2213.889..2213.889 rows=1 loops=1)
--   ->  Seq Scan on test  (cost=0.00..201492.00 rows=10000000 width=8) (actual time=0.029..1330.345 rows=10000000 loops=1)
-- Planning time: 0.589 ms
-- Execution time: 2213.908 ms

set max_parallel_workers_per_gather = 2;
explain analyze select min(ts) from test;
-- Finalize Aggregate  (cost=154575.72..154575.73 rows=1 width=8) (actual time=1093.310..1093.310 rows=1 loops=1)
--   ->  Gather  (cost=154575.50..154575.71 rows=2 width=8) (actual time=1093.252..1093.308 rows=3 loops=1)
--         Workers Planned: 2
--         Workers Launched: 2
--         ->  Partial Aggregate  (cost=153575.50..153575.51 rows=1 width=8) (actual time=1084.666..1084.666 rows=1 loops=3)
--               ->  Parallel Seq Scan on test  (cost=0.00..143158.80 rows=4166680 width=8) (actual time=0.037..809.809 rows=3333333 loops=3)
-- Planning time: 0.271 ms
-- Execution time: 1136.364 ms

-- works ! TODO: graph.
set max_parallel_workers_per_gather = 7;
explain analyze select min(ts) from test;
-- Execution time: 1060.065 ms

-- join test
drop table test;

set max_parallel_workers_per_gather = 0;

explain select p.id, c.gid, st_intersection(p.geom, c.geom) as geom
from parcelle p
  join carreau c on st_intersects(p.geom, c.geom)
where p.annee = 2015;


-- TODO:
-- tests:
-- FMI 4 queries
-- PP 4 queries
-- PG// : 4 workers
-- => 3 tables, 3 times

-- run-all.bash:
-- grep "execution time" run-all.log
-- FMI execution time: 146 s.
-- Par-psql execution time: 89 s.
-- PG // execution time: 114 s.
-- Total execution time: 349 s.
-- Pas de pg //

-- 8 workers:
-- FMI execution time: 306 s.
-- Par-psql execution time: 83 s.
-- PG // execution time: 113 s.
-- Total execution time: 502 s.
-- Pas de pg //

--conf FMI: split:20 jobs:6
-- FMI Execution time: 1261 s.
--  Execution time: 40734.295 ms
-- PG // Execution time (%WORKERS% workers): 41 s.
--  Execution time: 97939.774 ms
-- PG // Execution time (%WORKERS% workers): 98 s.
--  Execution time: 86570.123 ms
-- PG // Execution time (%WORKERS% workers): 86 s.
--  Execution time: 78292.100 ms
-- PG // Execution time (%WORKERS% workers): 79 s.
-- Total Execution time: 1566 s.

select count(*) from inter_sample_fmi;
select count(*) from inter_sample_pgpar;
select count(*) from inter_sample_pp;

-------------------------------------------------------
-- preparation small sample table to test queries:
-- sud ouest: 
-- 'BOX(296370 6116586,785596 6507260)'::box2d;
-- hg full:
-- 'BOX(491450 6204564,632729 6317384)'::box2d;
-- hg big
-- BOX(553199 6255599, 591802 6288111)::box2d;
-- hg small
-- BOX(557340 6271009,570323 6281944)::box2d;

drop table if exists parcelle_sample0;
create table parcelle_sample0 as select * from parcelle
where geom && 'BOX(296370 6116586,785596 6507260)'::box2d;

alter table parcelle_sample0 add PRIMARY KEY (id);
create index parcelle_sample0_geom_gist on parcelle_sample0 USING gist(geom);
VACUUM ANALYSE parcelle_sample0;

drop table if exists carreau_sample0;
create table carreau_sample0 as select * from carreau
where geom && 'BOX(296370 6116586,785596 6507260)'::box2d;

alter table carreau_sample0 add PRIMARY KEY (id);
create index carreau_sample0_geom_gist on carreau_sample0 USING gist(geom);
VACUUM ANALYSE carreau_sample0;

select count(*) from carreau_sample0;
select count(*) from parcelle_sample0;

drop table if exists parcelle_sample1;
create table parcelle_sample1 as select * from parcelle
where geom && 'BOX(491450 6204564,632729 6317384)'::box2d;

alter table parcelle_sample1 add PRIMARY KEY (id);
create index parcelle_sample1_geom_gist on parcelle_sample1 USING gist(geom);
VACUUM ANALYSE parcelle_sample1;

drop table if exists carreau_sample1;
create table carreau_sample1 as select * from carreau
where geom && 'BOX(491450 6204564,632729 6317384)'::box2d;

alter table carreau_sample1 add PRIMARY KEY (id);
create index carreau_sample1_geom_gist on carreau_sample1 USING gist(geom);
VACUUM ANALYSE carreau_sample1;

select count(*) from carreau_sample1;
select count(*) from parcelle_sample1;

drop table if exists parcelle_sample2;
create table parcelle_sample2 as select *, clock_timestamp() as creation_time from parcelle
where geom && 'BOX(553199 6255599, 591802 6288111)'::box2d;

alter table parcelle_sample2 add PRIMARY KEY (id);
create index parcelle_sample2_geom_gist on parcelle_sample2 USING gist(geom);
VACUUM ANALYSE parcelle_sample2;

drop table if exists carreau_sample2;
create table carreau_sample2 as select * from carreau
where geom && 'BOX(553199 6255599, 591802 6288111)'::box2d;

alter table carreau_sample2 add PRIMARY KEY (id);
create index carreau_sample2_geom_gist on carreau_sample2 USING gist(geom);
VACUUM ANALYSE carreau_sample2;

select count(*) from carreau_sample2;
select count(*) from parcelle_sample2;

drop table if exists parcelle_sample3;
create table parcelle_sample3 as select * from parcelle
where geom && 'BOX(557340 6271009,570323 6281944 )'::box2d;

alter table parcelle_sample3 add PRIMARY KEY (id);
create index parcelle_sample3_geom_gist on parcelle_sample3 USING gist(geom);
VACUUM ANALYSE parcelle_sample3;

drop table if exists carreau_sample3;
create table carreau_sample3 as select * from carreau
where geom && 'BOX(557340 6271009,570323 6281944 )'::box2d;

alter table carreau_sample3 add PRIMARY KEY (id);
create index carreau_sample3_geom_gist on carreau_sample3 USING gist(geom);
VACUUM ANALYSE carreau_sample3;

select count(*) from carreau_sample3;
select count(*) from parcelle_sample3;

-- Table switch to do before launching tests:
drop table if exists parcelle_sample;
create table parcelle_sample (like parcelle_sample1 INCLUDING all);
insert into parcelle_sample 
  select * from parcelle_sample1;

VACUUM ANALYSE parcelle_sample;

drop table if exists carreau_sample;
create table carreau_sample (like carreau_sample1 INCLUDING all);
insert into carreau_sample 
  select * from carreau_sample1;

VACUUM ANALYSE carreau_sample;

-- swap big table
alter table parcelle RENAME to parcelle_sample;
alter table carreau RENAME to carreau_sample;

------------------------------------------------------------------------------------------
drop TABLE if EXISTS inter_sample;
CREATE TABLE inter_sample as
WITH tmp as (
    SELECT
      p.id                                                                      AS idparc,
      c.gid                                                                     AS idcarreau,
      p.annee,
      st_multi(st_intersection(p.geom, c.geom)) :: GEOMETRY(MULTIPOLYGON, 2154) AS geom,
      clock_timestamp()                    AS creation_time
    FROM parcelle_ssample p
      JOIN carreau_ssample c ON st_intersects(p.geom, c.geom)
) select
    idparc,
    idcarreau,
    annee,
    geom,
    creation_time,
    to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt
  from tmp t ;

alter table inter_sample add column end_time_txt text;
update inter_sample set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from inter_sample);
VACUUM ANALYSE inter_sample;

select count(*) from inter_sample;

select min(creation_time), max(creation_time), min(creation_time_txt), max(creation_time_txt)
from inter_sample;

table inter_sample;
-- 3 sec frame
-- time format: %Y-%m-%d %H:%M:%S.%f

--  /usr/local/pgsql-9.6/bin/pgsql2shp -f inter.shp -p 5439 -u test -b -r nicolas "select p.id as idparc, c.gid as idcarreau, p.annee, st_multi(st_intersection(p.geom, c.geom))::geometry(MULTIPOLYGON , 2154) as geom,  (extract(EPOCH from clock_timestamp())*100)::bigint as creation_time from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)"


drop table inter_fmi;
-- 74 765 369
drop table inter_pp;
-- 74765369

select count(*) from inter_pgpar;

-- on big data:
-- FMI Execution time: 10831 s. 180min
-- Par-psql Execution time: 3883 s. 64 min
-- PG // Execution time (6 workers): 15122 s. 4h20
-- Total Execution time: 29837 s. 8h28

-- TODO questions: FMI trop lent: trouver les bons params

-- 15 sept 2016:
-- sample dataset:
-- HG

select count(*) from inter_sample_fmi;
select count(*) from inter_sample_pp;
select count(*) from inter_sample_pgpar;

alter table inter_pgpar add column cent_geom geometry(point, 2154);
UPDATE inter_pgpar set cent_geom = st_centroid(geom);
create index inter_pgpar_cent_geom_gist on inter_pgpar USING GIST (cent_geom);

alter table inter_pgpar add column creation_time_txt text;
alter table inter_pgpar add column end_time_txt text;
update inter_pgpar set creation_time_txt = to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS');
update inter_pgpar set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from inter_pgpar);

alter table inter_pgpar add column ct DOUBLE PRECISION;
update inter_pgpar set ct = extract (EPOCH from creation_time);


VACUUM ANALYSE inter_pgpar;

-- TODO: cluster de pts pour affichage => 10x moins
-- spatial agg for cluster.
--
select count(*) from carreau_sample;
alter table carreau_sample add column cent_geom geometry(point, 2154);
UPDATE carreau_sample set cent_geom = st_centroid(geom);
create index carreau_sample_cent_geom_gist on carreau_sample USING GIST (cent_geom);
VACUUM ANALYSE carreau_sample;

DROP table IF EXISTS testcluster;
create table testcluster as
  SELECT row_number() over () as id,
    unnest(ST_ClusterWithin(cent_geom, 400)) as geom
  FROM inter_pgpar;

select pg_size_pretty(pg_relation_size('inter_pgpar'));

set max_parallel_workers_per_gather = 6;

set ENABLE_SEQSCAN to on;
explain ANALYSE SELECT ST_ClusterWithin(cent_geom, 400) as geom
FROM inter_pgpar;
-- no // plan


-- test: group by temporal range:
set parallel_setup_cost	to 1000;
set parallel_setup_cost	to 1;

DROP table IF EXISTS testcluster;
create table testcluster as
  SELECT row_number() over () as id,
    COUNT(*) cnt,
--     to_timestamp(floor((extract('epoch' from creation_time) / 0.01 )) * 0.01) as interval_alias,
    floor((extract('epoch' from creation_time) / 0.01 )) * 0.01 as interval_alias,
     st_centroid(st_collect(geom))::geometry(point, 2154) as geom
--      st_envelope(st_collect(geom))::geometry(polygon, 2154) as geom
--     st_convexhull(st_collect(geom))::geometry(polygon, 2154) as geom
  FROM inter_pgpar_big
  GROUP BY interval_alias;

select count(*)
  from testcluster;

explain analyse select sum(st_numgeometries(geom))
from testcluster;

explain analyse select sum(st_numgeometries(geom))
from parcelle;

select id, st_numgeometries(unnest) as size, st_envelope(unnest) as geom
from testcluster;
-- cool

select count(*) from parcelle_sample0;
-- 22125334
select count(*) from carreau_sample0;
-- 545280

select count(*) from parcelle_sample1;
-- 2892689
select count(*) from carreau_sample1;
-- 87376

select count(*) from parcelle_sample2;
-- 376131
select count(*) from carreau_sample2;
-- 14354

select * from create_table_parallel(
    'inter_pgpar',
    'select p.id as idparc, c.gid as idcarreau, p.annee,
        clock_timestamp() AS creation_time,
        st_intersection(p.geom, c.geom) as geom
      from parcelle_sample p
      join carreau_sample c on st_intersects(p.geom, c.geom)',
    '/usr/local/pgsql-9.6/bin/psql -A -t -p 5439 -d nicolas -c',
    6,
    true);
-- sample2: 29s vs 88s
-- sample1: 1m 48s 120ms vs 6m 48s 211ms
-- sample0: 12m 20s 428ms vs

-- timing explain analyse:
set max_parallel_workers_per_gather = 0;
set max_parallel_workers_per_gather = 6;
explain select p.id as idparc, c.gid as idcarreau
from parcelle_sample2 p
join carreau_sample2 c on st_intersects(p.geom, c.geom);
-- 8w: Execution time: 22 ms vs Execution time: 82 ms

-- // plans for seqscan, join, agregate:

-- spatial // equivalent:
-- table stats:
set max_parallel_workers_per_gather = 0;
explain select sum(st_npoints(geom)), min(st_npoints(geom)), max(st_npoints(geom)), avg(st_npoints(geom)) from parcelle;

-- Aggregate  (cost=15492093.56..15492093.57 rows=1 width=48)
--   ->  Seq Scan on parcelle  (cost=0.00..4694615.88 rows=98158888 width=253)

set max_parallel_workers_per_gather = 6;
explain analyze select sum(st_npoints(geom)), min(st_npoints(geom)), max(st_npoints(geom)), avg(st_npoints(geom)) from parcelle;
-- Finalize Aggregate  (cost=5677205.47..5677205.48 rows=1 width=48)
--   ->  Gather  (cost=5677204.80..5677205.41 rows=6 width=48)
--         Workers Planned: 6
--         ->  Partial Aggregate  (cost=5676204.80..5676204.81 rows=1 width=48)
--               ->  Parallel Seq Scan on parcelle  (cost=0.00..3876625.15 rows=16359815 width=253)


-- Finalize Aggregate  (cost=5677205.47..5677205.48 rows=1 width=48) (actual time=107645.180..107645.180 rows=1 loops=1)
--   ->  Gather  (cost=5677204.80..5677205.41 rows=6 width=48) (actual time=107642.451..107642.647 rows=7 loops=1)
--         Workers Planned: 6
--         Workers Launched: 6
--         ->  Partial Aggregate  (cost=5676204.80..5676204.81 rows=1 width=48) (actual time=107633.094..107633.094 rows=1 loops=7)
--               ->  Parallel Seq Scan on parcelle  (cost=0.00..3876625.15 rows=16359815 width=253) (actual time=0.050..80969.978 rows=13999771 loops=7)
-- Planning time: 0.064 ms
-- Execution time: 107645.280 ms

-- => diff: cost=15492093.56..15492093.57 vs 5677204.80..5677205.41
-- different planned and configured workers values according to max_workers and table size.

set max_parallel_workers_per_gather = 0;
set max_parallel_workers_per_gather = 6;
set parallel_setup_cost	to 1000;
set parallel_setup_cost	to 0.01;

explain select sum(st_npoints(geom)) from parcelle;
explain select array_agg(geom) from parcelle;

explain select st_accum(geom) from parcelle;
explain select st_extent(geom) from parcelle;
explain select st_union(geom) from parcelle;
explain select st_memunion(geom) from parcelle;
explain select st_collect(geom) from parcelle;
explain select st_collect(array_agg(geom)) from parcelle;
explain select st_clusterwithin(geom, 2000) from parcelle;

explain with tmp as (
  select array_agg(geom) as a from parcelle
) select st_union(a) from tmp;

-- join
explain select p.id, c.gid
from parcelle p join carreau c on st_intersects(p.geom, c.geom);
-- Gather  (cost=1000.41..11022271223.12 rows=74542277448 width=12)
--   Workers Planned: 6
--   ->  Nested Loop  (cost=0.41..3568042478.32 rows=74542277448 width=12)
--         ->  Parallel Seq Scan on parcelle p  (cost=0.00..3876625.15 rows=16359815 width=261)
--         ->  Index Scan using carreau_geom_idx on carreau c  (cost=0.41..217.10 rows=76 width=36)
--               Index Cond: (p.geom && geom)
--               Filter: _st_intersects(p.geom, geom)

explain select c.gid, st_union(p.geom)
from parcelle p join carreau c on st_intersects(p.geom, c.geom)
group by c.gid;


-- qgis display of 3 sample2 tables:
-- illustrates object creation time:
alter table inter_pgpar2 add column creation_time_txt text;
alter table inter_pgpar2 add column end_time_txt text;
update inter_pgpar2 set creation_time_txt = to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS');
update inter_pgpar2 set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from inter_pgpar2);
VACUUM ANALYSE inter_pgpar2;

alter table inter_pgpar2_seq add column creation_time_txt text;
alter table inter_pgpar2_seq add column end_time_txt text;
update inter_pgpar2_seq set creation_time_txt = to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS');
update inter_pgpar2_seq set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from inter_pgpar2_seq);
VACUUM ANALYSE inter_pgpar2_seq;

alter table parcelle_sample2 add column creation_time_txt text;
alter table parcelle_sample2 add column end_time_txt text;
update parcelle_sample2  set creation_time_txt = to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS');
update parcelle_sample2  set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from parcelle_sample2);
VACUUM ANALYSE parcelle_sample2;

-- creation sequentielle : %Y-%m-%d %H:%M:%S.%f
drop table if exists inter_pgpar_seq;
CREATE TABLE inter_pgpar_seq as (
  SELECT
    p.id                            AS idparc,
    c.gid                           AS idcarreau,
    p.annee,
    clock_timestamp()               AS creation_time,
    st_intersection(p.geom, c.geom) AS geom
  FROM parcelle_sample p
    JOIN carreau_sample c ON st_intersects(p.geom, c.geom)
);

-- cluster parcelle_sample2 to reorder geom:
CLUSTER parcelle_sample2_geom_gist on parcelle_sample2;
CLUSTER parcelle_sample2;
VACUUM ANALYSE parcelle_sample2;

CREATE INDEX parcelle_sample2_myhash_idx ON parcelle_sample2 (ST_GeoHash(st_transform(geom, 4326)));
CLUSTER parcelle_sample2 USING parcelle_sample2_myhash_idx;

-- num workers involved, for stats:
-- sample2:
set max_parallel_workers_per_gather = 6;
create table inter_pgpar3 as
  select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample3 p
  join carreau_sample3 c on st_intersects(p.geom, c.geom);
-- 6w: 3 3: 21.7 s
-- 2w: 2 2: 28 s
-- 1w: 0 0: 82 s

-- sample1:
set max_parallel_workers_per_gather = 6;
explain ANALYSE select p.id as idparc, c.gid as idcarreau, p.annee,
    clock_timestamp() AS creation_time,
    st_intersection(p.geom, c.geom) as geom
  from parcelle_sample1 p
  join carreau_sample1 c on st_intersects(p.geom, c.geom);


-- test cleaning with // queries:
set max_parallel_workers_per_gather = 6;
explain analyse select st_isvalidReason(geom)
from parcelle_sample2 where not st_isvalid(geom);
-- Execution time: 17528.797 ms vs Execution time: 4993.032 ms :))

-- clean //  13m 51s 395ms
-- clean classic: 30m 32s 955ms

select pg_size_pretty(pg_database_size('nicolas'));


