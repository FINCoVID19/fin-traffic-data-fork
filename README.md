# fin_traffic_data

Python package for:

1. fetching raw historical traffic data from
   [Finnish Transport Infrastructure Agency](https://vayla.fi),
2. aggregating said data,
3. building directional graph between areas and mapping the edges to the appropriate sections of the aggregated traffic data, and
4. visualizing the aggregated data.

## Disclaimer

This repo is forked from the original work by Janne Solanpää. See the repository [here](https://gitlab.com/solanpaa/fin-traffic-data).

## Installation

This package has been tested for Python >= 3.6

Use the command.
```sh
pip install -e .
```

There is a version that differs from this one available in [PyPi](https://pypi.org/project/fin-traffic-data/). To install use:
```sh
pip3 install fin-traffic-data
```

## Usage

### Fetching raw traffic data


The console script `fin-traffic-fetch-raw-data` allows you to fetch the raw
traffic data of all traffic measuring stations between two dates.

```sh
fin-traffic-fetch-raw-data --begin-date 2020-01-01 --end-date 2020-02-01
```

The dates are formatted as YYYY-MM-DD. The script spits out HDF5 files storing 
pandas dataframes with the filenaming convention `fin_traffic_raw_<begin-date>_<end_date>.h5`.

The output file contains the raw traffic data for each TMS in a dataset called
`tms_<tms id>`.

### Aggregating raw data

The console script `fin-traffic-aggregate-raw-data` allows you the aggregate pre-fetched
traffic data. Usage::

```sh
fin-traffic-aggregate-raw-data --dir raw_data/ --time-resolution 1h
```

Here the options are

`--dir`
    Directory from which to load the datafiles for raw traffic data

`--time-resolution`
    Time-resolution of the aggregation. Use the literals `w` for weeks,
    `d` for days, and `h` for hours.

The script spits out a file named `fin_traffic_aggregated_<begin-date>_<end-date>_<time-resolution>.h5`.


### Computing traffic between provinces and university hospital catchment areas

The console script `fin-traffic-compute-traffic-between-areas` can be used to compute 
traffic between different regions. For computing traffic between provinces, use the command::

```sh
fin-traffic-compute-traffic-between-areas \
--area <area> \
--input <path_to_the_time_aggregated_file>
```

For the parameter `<area>` there are 3 possible choices:
- `erva`: university hospital catchment areas
- `province`: finland provinces
- `hcd`: Hospital Care District areas.
The parameter `<path_to_the_time_aggregated_file>` is the path to the output file of the command `fin-traffic-aggregate-raw-data`

This tool spits out a file named `tms_between_<area>s_<begin-date>_<end-date>_<time-resolution>.h5` in a default folder named `aggregated_data_hcd`.

An example of running the command would be:

```sh
fin-traffic-compute-traffic-between-areas \
--area hcd \
--input aggregated_data/fi_traffic_aggregated-2020-01-01 00:00:00-2020-02-01 00:00:00-1:00:00.h5
```

### Converting `<area>` level traffic to CSV format

For converting `<area>` level traffic to a compressed archive of CSV-files, use the command

```sh
fin-traffic-export-traffic-between-areas-to-csv \
--input <path_to_aggregated_traffic_file>
```

This requires the file generated by the command `fin-traffic-compute-traffic-between-areas`. Assuming one has ran the command and kept the default folder as the result folder we could issue the command:

```sh
fin-traffic-export-traffic-between-areas-to-csv \
--input aggregated_data_hcd/tms_between_hcds_fi_traffic_aggregated-2020-01-01 00:00:00-2020-02-01 00:00:00-1:00:00.h5
```

### Running the complete pipeline

The complete pipeline can be ran using the command

```sh
fin-traffic-complete_pipeline \
--begin-date <being_date> \
--end-date <end_date> \
--time-resolution <time-resolution> \
--aggregation_level <area> \
--results_dir_fetch <path/to/dir> \
--results_dir_aggregate <path/to/dir> \
--results_dir_traffic <path/to/dir>
```

Example:

```sh
fin-traffic-complete_pipeline \
--begin-date 2020-01-01 \
--end-date 2020-02-01 \
--time-resolution 1h \
--aggregation_level hcd \
--results_dir_fetch ~/Documents/foo/raw_data \
--results_dir_aggregate ~/Documents/foo/aggregate_time \
--results_dir_traffic ~/Documents/foo/aggregate_hcd
```

### Schedule a daily download of the data

We can also use a *schedule* to daily check for new data. What the *schedule* does is to check **hourly** for data of the day before. Specifically, it gets the system time and checks the hour, if it's before 12pm then it goes back to sleep. If it's after 12 pm, it will try to get all the new data between the last download time and the day before and then go back to sleep for one hour.

Example:
```sh
fin-traffic-schedule-complete_pipeline --begin-date 2020-01-01 \
--time-resolution 1h --aggregation_level hcd \
--results_dir_fetch ~/Documents/foo/raw_data \
--results_dir_aggregate ~/Documents/foo/aggregate_time \
--results_dir_traffic ~/Documents/foo/aggregate_hcd
```

**N.B:** The script/command does not clean the old files. This can cause that there are old files with repeated data occupying disk space. The folders should be cleaned by hand or maybe later in the future implement a functionality to clean them automatically.
