#!/usr/bin/env bash

# test trigger // query with write capabilities:

query="select p.id as idparc, c.gid as idcarreau, p.annee, st_intersection(p.geom, c.geom) as geom from parcelle_sample p join carreau_sample c on st_intersects(p.geom, c.geom)"
#query="select idparc, idcarreau, annee, geom from inter_sample"

/usr/local/pgsql-9.6/bin/psql -A -t -p 5439 -U test -d nicolas -c "${query}"
