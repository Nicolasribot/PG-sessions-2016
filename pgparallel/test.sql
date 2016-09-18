-- Fichier test pour requete en // avec pg:
-- cf. todo.sql pour les data.
--
--
-- set max_parallel_workers_per_gather = 0;
-- set max_parallel_workers_per_gather = %WORKERS%;

-- drop table if exists inter_sample_pgpar;
--
-- explain create table inter_sample_pgpar as (
--   SELECT
--     p.id                            AS idparc,
--     c.gid                           AS idcarreau,
--     p.annee,
--     st_intersection(p.geom, c.geom) AS geom
--   FROM parcelle_sample p
--     JOIN carreau_sample c ON st_intersects(p.geom, c.geom)
-- );
--
-- select count(*)
-- from inter_sample_pgpar;

select 'num workers set: %WORKERS%';

drop table if exists inter_pp;

select * from create_table_parallel(
    'inter_big',
    'select p.id as idparc, c.gid as idcarreau, p.annee,
        clock_timestamp() AS creation_time,
        st_intersection(p.geom, c.geom) as geom
      from parcelle_sample p
      join carreau_sample c on st_intersects(p.geom, c.geom)',
    '/usr/local/pgsql-9.6/bin/psql -A -t -p 5439 -d nicolas -c',
    %WORKERS%,
    true);

