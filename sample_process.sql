-- full sql process on sample data:
set max_parallel_workers_per_gather = 0;

------------------------------------------------------------------------------------------------------------------------
-- tp de nettoyage des données par postgis pur (sans topo)
-- extrait de la table parcelle: parc_sample1
-- extent: BOX(657098 6861118,662058 6864439)
--
-- But: identifier les pg en intersection
-- tenter de nettoyer les intersections:
--
-- Idée: pour chaque intersection entre 2 polygones, si on veut rendre les deux pg jointifs, il faut faire l'union
-- du pg 1 et de l'intersection, et ensuite la difference du pg 2 et de l'intersection.
-- dans le cas général, un polygone peut intersecter plusieurs autres, et un meme polygone
-- peut etre concerné par une union avec certains bouts en intersection, et une différence avec d'autres parties en
-- intersection (le choix est aléatoire: chaque partie en intersection devant etre traitée sur chaque pg concerné)
--
-- Attention: Avec une requete self join sur la table des parcelles, on va identifier deux fois les couples de pg en intersection:
-- pg1 inter pg2 et pg2 inter pg2. Il ne faut garder qu'un exemplaire de chaque couple de pg a traiter.
--
-- La correction de chaque parcelle concernée par une intersection correspond a l'enchainement de:
--  - l'union de la parcelle avec les parties en intersection choisies pour l'union
--  - la difference de la parcelle avec les parties en intersection choisies pour la difference
-- Ce choix union/difference est arbitraire et se base sur le couple de gid concernés par une intersection
-- ... st_union(p.geom, uniongeom), diffgeom)

-- on veut donc une table listant, pour chaque parcelle concernée par une intersection,
-- les geometries a ajouter et celles a soustraire:
-- on utilise une geom vide, geometry empty, qd l'info est manquante => on traite toutes les geom
-- avec la meme requete en une seule passe


--  découpage du procesus en tables intermédiaires pour l'exercice. possibilité de créer des tables unlogged
-- pour aller plus vite (mais perte de la table en cas de crash de PG)
--
------------------------------------------------------------------------------------------------------------------------
-- create EXTENSION intarray;

-- on crée un type composite qui represente un gid de pg et l'operation le concernant:
-- drop type if EXISTS pgop_type cascade;
-- create type pgop_type AS (
--   gid int,
--   op int
-- );

-- pg en intersection: on garde une table des id en intersection et du pg intersection
-- union directe de tous les morceaux:
-- puis la table donnant les parcelles, leurs operations et les geom les concernants (union des geom)

-- test inner // ?

drop table if exists inter2;
create UNLOGGED table inter2 as (
  with tmp as (
    SELECT
      unnest(ARRAY [(p1.id, 1)::pgop_type, (p2.id, 2)::pgop_type]) as geom_ope,
      st_intersection(p1.geom, p2.geom) AS intergeom
    FROM parcelle_sample1 p1 JOIN parcelle_sample1 p2 ON st_overlaps(p1.geom, p2.geom) AND p1.id < p2.id
    )
  select (t.geom_ope).gid, (t.geom_ope).op, st_union(intergeom) as intergeom
  FROM tmp t
  GROUP BY (t.geom_ope).gid, (t.geom_ope).op
);
-- 7s

-- check validite, corrections éventuelles
-- select st_isvaliddetail(intergeom), st_isvalidreason(intergeom)
-- from inter2
-- where not st_isvalid(intergeom);

alter table inter2 add column id serial PRIMARY KEY ;
create index inter2_gid_idx on inter2 (gid);
create index inter2_geom_gist on inter2 USING GIST (intergeom);
VACUUM ANALYZE inter2;

-- a partir de cette table, on construit une table contenant chaque id de parcelle a traiter
-- et trois colonnes: la geom initiale de la parcelle a corriger, uniongeom, diffgeom: les geom concernées par chaque opération
-- Si une opération n'est pas disponible pour la parcelle en question, on force la géométrie
-- à une GEOMETRYCOLLECTION EMPTY: ca permet d'appeler tout le temps la meme opération postgis
-- et de ne pas toucher a la géometrie de départ si une opération ne doit pas etre faite:
-- si on mettait null, le résultat de l'op serait null => geom parcelle mise a null !
-- TODO: exercice: faire cette table par requete directe dans la creation de la table inter:
-- smart way: grouping sets ?
drop table if exists parc_to_clean;
create table parc_to_clean as
with tmp as (
 SELECT
      t.gid,
      array_agg(t.op ORDER BY t.op) AS ops,
      array_agg(t.intergeom ORDER BY t.op) AS uniondiffgeom,
      p.geom
    FROM inter2 t
      JOIN parcelle_sample1 p ON t.gid = p.id
    GROUP BY t.gid, p.geom
), tmp1 as (
    SELECT
      t.gid,
      t.ops,
      t.geom,
      -- todo: smarter !
      CASE WHEN array_length(t.ops, 1) = 1 AND t.ops [1] = 1
        THEN uniondiffgeom || 'GEOMETRYCOLLECTION EMPTY' :: GEOMETRY
      WHEN array_length(t.ops, 1) = 1 AND t.ops [1] = 2
        THEN 'GEOMETRYCOLLECTION EMPTY' :: GEOMETRY || uniondiffgeom
      ELSE uniondiffgeom END AS uniondiffgeom
    FROM tmp t
) select t.gid,
  st_difference(st_union(t.geom, t.uniondiffgeom[1]), t.uniondiffgeom[1]) as geom
  from tmp1 t;

-- delete from main table from this table
create index parc_to_clean_gid_idx on parc_to_clean(gid);
VACUUM ANALYSE parc_to_clean;

DROP TABLE IF EXISTS parcelle_sample1_clean;
create table parcelle_sample1_clean as  (
  select p.id, p.idpar, p.annee, p.creation_time,
    coalesce (pc.geom, p.geom) as geom
  from parcelle_sample1 p left join parc_to_clean pc on p.id = pc.gid
);

 --2m 47s 483ms 167