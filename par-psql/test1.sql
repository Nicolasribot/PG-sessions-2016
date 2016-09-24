-- tests par-psql with parcelle/carreau intersection
--

-- 4 // queries:
drop table if exists inter_pp_1; --&
drop table if exists inter_pp_2; --&
drop table if exists inter_pp_3; --&
drop table if exists inter_pp_4; --&
drop table if exists inter_pp_5; --&
drop table if exists inter_pp_6; --&
drop table if exists inter_pp_7; --&
drop table if exists inter_pp_8; --&

CREATE UNLOGGED TABLE inter_pp_1 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=0; --&

CREATE UNLOGGED TABLE inter_pp_2 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=1; --&

CREATE UNLOGGED TABLE inter_pp_3 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=2; --&

CREATE UNLOGGED TABLE inter_pp_4 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=3; --&

CREATE UNLOGGED TABLE inter_pp_5 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=4; --&

CREATE UNLOGGED TABLE inter_pp_6 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=5; --&

CREATE UNLOGGED TABLE inter_pp_7 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=6; --&

CREATE UNLOGGED TABLE inter_pp_8 as
  select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom
  ,    clock_timestamp()                    AS creation_time
  from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)
  where p.id%8=7; --&

drop table if exists inter_pp;
CREATE TABLE inter_pp as
  select *, to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_1
  UNION ALL
  select *, to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_2
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_3
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_4
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_5
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_6
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_7
  UNION ALL
  select *,to_char(creation_time, 'YYYY-MM-DD HH24:MI:SS.MS')                    AS creation_time_txt from inter_pp_8;

drop table inter_pp_1; --&
drop table inter_pp_2; --&
drop table inter_pp_3; --&
drop table inter_pp_4; --&
drop table inter_pp_5; --&
drop table inter_pp_6; --&
drop table inter_pp_7; --&
drop table inter_pp_8; --&

-- 87s
-- select count(*) from inter_pp;
-- -- 749426 -- ok !
-- alter table inter_pp add column end_time_txt text;
-- update inter_pp set end_time_txt = (select to_char(max(creation_time), 'YYYY-MM-DD HH24:MI:SS.MS') from inter_pp);
-- VACUUM ANALYSE inter_pp;
