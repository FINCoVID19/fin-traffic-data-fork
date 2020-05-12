#!/bin/sh
export PATH=$PWD/venv/bin:$PATH

YESTERDAY=$(date -I -d "yesterday")
TODAY=$(date -I -d "today")

fin-traffic-fetch-raw-data --begin-date ${YESTERDAY} --end-date ${TODAY}

fin-traffic-aggregate-raw-data --time-resolution 1h

rm tms_between*.h5

FILENAME="aggregated_data/fin-traffic-1:00:00-2020-01-01-${TODAY}.h5"
fin-traffic-compute-traffic-between-areas --input ${FILENAME} --area erva
fin-traffic-compute-traffic-between-areas --input ${FILENAME} --area province

rm tms*.tar.bz2
fin-traffic-export-traffic-between-areas-to-csv --area erva
fin-traffic-export-traffic-between-areas-to-csv --area province

# Upload the where-ever
