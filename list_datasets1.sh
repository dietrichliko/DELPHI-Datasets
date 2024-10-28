#!/bin/bash
#
# List energies associate to a year of DELPHI running

db_file="/afs/cern.ch/work/d/dliko/delphi-datasets.db"

year=${1-"1995"}

echo "Data ${year}"
sqlite3 "$db_file" <<ENDOFSQL
SELECT datasets.name, energies.value
FROM datasets
INNER JOIN association_table_2 ON datasets.id = association_table_2.dataset_id
INNER JOIN years ON association_table_2.year_id = years.id
INNER JOIN association_table_1 ON datasets.id = association_table_1.dataset_id
INNER JOIN association_table_4 ON association_table_1.file_id = association_table_4.file_id
INNER JOIN energies ON energies.id = association_table_4.energy_id
WHERE years.name = "${year}" and datasets.data = TRUE and datasets.name not like 'rawd%'
GROUP BY datasets.name
ENDOFSQL

echo "Simulation ${year}"
sqlite3 "$db_file" <<ENDOFSQL
SELECT datasets.name, energies.value
FROM datasets
INNER JOIN association_table_2 ON datasets.id = association_table_2.dataset_id
INNER JOIN years ON association_table_2.year_id = years.id
INNER JOIN association_table_1 ON datasets.id = association_table_1.dataset_id
INNER JOIN association_table_4 ON association_table_1.file_id = association_table_4.file_id
INNER JOIN energies ON energies.id = association_table_4.energy_id
WHERE years.name = "${year}" and datasets.data = FALSE
GROUP BY datasets.name
ENDOFSQL

