import argparse
from datetime import timedelta
import pathlib
import re

from fin_traffic_data.metadata import *
from fin_traffic_data.aggregation import *
from fin_traffic_data.utils import daterange

def _parse_time_resolution(x):
    """Parse human-readable input of time resolution"""
    m = re.findall(r"(?P<num>\d+)(?P<qualif>w|d|h|m|s)", x)
    dt = timedelta()
    for obj in m:
        if obj[1] == 'w':
            dt += timedelta(days=int(obj[0])*7)
        elif obj[1] == 'd':
            dt += timedelta(days=int(obj[0]))
        elif obj[1] == 'h':
            dt += timedelta(hours=int(obj[0]))
        elif obj[1] == 'm':
            dt += timedelta(minutes=int(obj[0]))
        elif obj[1] == 's':
            dt += timedelta(seconds=int(obj[0]))
        else:
            raise ValueError(f"Unknown literal '{obj[1]}'")
    return dt

def main():
    parser = argparse.ArgumentParser(
        description="Aggregates pre-loaded raw traffic data.")
    parser.add_argument("--dir", type=str,
                        help="Directory containing the raw traffic data",
                        default="raw_data")

    parser.add_argument("--time-resolution", type=_parse_time_resolution,
                        required=True,
                        help="Time resolution of the aggregation")

    args = parser.parse_args()

    basepath = args.dir
    delta_t = args.time_resolution

    # Create the output directory
    pathlib.Path("aggregated_data/").mkdir(parents=True, exist_ok=True)

    # Get the raw datafiles
    raw_data_files = list_rawdata_files(basepath)
    if not raw_data_files:
        raise RuntimeError(f"No datafiles in {basepath}/")

    # Sanity checking of the files with raw traffic data
    check_no_daterange_overlap_in_raw_files(raw_data_files)
    check_all_dates_covered_by_raw_files(raw_data_files)

    # Get list of all TMS stations
    all_tms_stations = get_tms_stations()

    # Aggregate all the datafiles
    aggregate_datafiles(
        raw_data_files,
        all_tms_stations['num'],
        delta_t
    )

if __name__ == '__main__':
    main()
