fin_traffic_data
================

Python package for:

1. fetching raw historical traffic data from
   `Finnish Transport Infrastructure Agency <https://vayla.fi>`_.
2. aggregating said data 
3. building bi-directional graph between municipalities and mapping
   the edges to the appropriate sections of the aggregated traffic data

Installation
------------

::

    pip3 install fin-traffic-data

Fetching raw traffic data
-------------------------

The console script `fi-traffic-fetch-raw-data` allows you to fetch the raw
traffic data of all traffic measuring stations between two dates. Usage::

    fin-traffic-fetch-raw-data --begin-date 2020-01-01 --end-date 2020-02-01

The dates are formatted as YYYY-MM-DD.
