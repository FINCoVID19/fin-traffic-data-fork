fin_traffic_data
================

Python package for:

1. fetching raw historical traffic data from
   `Finnish Transport Infrastructure Agency <https://vayla.fi>`_,
2. aggregating said data,
3. building directional graph between provinces/ervas and mapping
   the edges to the appropriate sections of the aggregated traffic data, and
4. visualizing the aggregated data.


Installation
------------

::

    pip3 install fin-traffic-data


Fetching raw traffic data
-------------------------

The console script `fin-traffic-fetch-raw-data` allows you to fetch the raw
traffic data of all traffic measuring stations between two dates. Usage::

    fin-traffic-fetch-raw-data --begin-date 2020-01-01 --end-date 2020-02-01

The dates are formatted as YYYY-MM-DD. The script spits out HDF5 files storing 
pandas dataframes with the filenaming convention `fin_traffic_raw_<begin-date>_<end_date>.h5`.

The output file contains the raw traffic data for each TMS in a dataset called
`tms_<tms id>`.

Aggregating raw data
--------------------

The console script `fin-traffic-aggregate-raw-data` allows you the aggregate pre-fetched
traffic data. Usage::
    
    fin-traffic-aggregate-raw-data --dir raw_data/ --time-resolution 1h

Here the options are

`--dir`
    Directory from which to load the datafiles for raw traffic data

`--time-resolution`
    Time-resolution of the aggregation. Use the literals `w` for weeks,
    `d` for days, and `h` for hours.

The script spits out a file named 
`fin_traffic_aggregated_<begin-date>_<end-date>_<time-resolution>.h5`.


Computing traffic between provinces and university hospital catchment areas
---------------------------------------------------------------------------

The console script `fin-traffic-compute-traffic-between-areas` can be used to compute 
traffic between different regions. For computing traffic between provinces, use the command::

    fin-traffic-compute-traffic-between-areas --area province --input aggregated_data/fi_traffic_aggregated-2020-01-01 00:00:00-2020-09-16 00:00:00-1:00:00.h5

For traffic between university hospital catchment areas, use the flag `--area erva`. This tool spits out a file named
`tms_between_ervas.h5` or `tms_between_provinces.h5`.


Converting province/ERVA level traffic to CSV format
----------------------------------------------------

For converting province/ERVA level traffic to a compressed archive of CSV-files, use the command::

    fin-traffic-export-traffic-between-areas-to-csv --area erva

This requires the file `tms_between_ervas.h5` and outputs the archive `tms_between_ervas.tar.bz2`.

