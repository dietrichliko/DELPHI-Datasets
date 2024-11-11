#!/bin/bash
#
# List energies associate to a year of DELPHI running

db_file="/afs/cern.ch/work/d/dliko/delphi-datasets.db"

name=${1}

sqlite3 "$db_file" <<ENDOFSQL
SELECT files.path
FROM files
INNER JOIN association_table_1 ON files.id = association_table_1.file_id
INNER JOIN datasets ON association_table_1.dataset_id = datasets.id
WHERE datasets.name = "${name}" COLLATE NOCASE
ENDOFSQL

