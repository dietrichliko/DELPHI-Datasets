#!/bin/bash
#
# List energies associate to a year of DELPHI running

db_file="/afs/cern.ch/work/d/dliko/delphi-datasets.db"

name=${1}

sqlite3 "$db_file" <<ENDOFSQL
SELECT datasets.name
FROM datasets
INNER JOIN association_table_1 AS a1 ON datasets.id = a1.dataset_id
INNER JOIN association_table_1 AS a2 ON a1.file_id = a2.file_id
INNER JOIN datasets AS d ON a2.dataset_id = d.id
WHERE d.name = "${name}" COLLATE NOCASE
GROUP BY datasets.name
ENDOFSQL

