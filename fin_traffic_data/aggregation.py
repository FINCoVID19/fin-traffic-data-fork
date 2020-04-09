import argparse
import datetime
from functools import reduce
from glob import glob
import itertools
import multiprocessing
import re
from typing import List, Text, Tuple

import pandas as pd
import numpy as np
import progressbar
import tqdm

from fin_traffic_data.utils import *


def list_rawdata_files(path: Text):
    filenames = glob(f"{path}/fin_traffic_raw*.h5")
    files = []
    for f in filenames:
        m = re.match(
            (r".*fin_traffic_raw_(?P<begin_date>\d{4}-\d{1,2}-\d{1,2})_"
             r"(?P<end_date>\d{4}-\d{1,2}-\d{1,2})\.h5"), f)
        if not m:
            raise RuntimeError(f"Invalid file name {f}")

        begin_date = datetime.datetime.strptime(m.group('begin_date'),
                                                "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(m.group('end_date'),
                                              "%Y-%m-%d").date()
        files.append((f, begin_date, end_date))
    return sorted(files, key=lambda s: s[1])


def check_no_daterange_overlap_in_raw_files(raw_files):
    """
    Checks if the raw datafiles have some common dates.

    Input
    -----
    raw_files: List[Tuple[Text, datetime.date, datetime.date]]

    Raises a RuntimeError if there are common dates.
    """
    for i, f in enumerate(raw_files):
        for f2 in raw_files[i + 1:]:
            overlap_dates = compute_daterange_overlap(f[1], f[2], f2[1], f2[2])
            if len(overlap_dates) != 0:
                raise RuntimeError(
                    f"Files {f[0]} and {f2[0]} have overlapping dates.")


def check_all_dates_covered_by_raw_files(raw_files):
    """
    Checks if all the dates are covered by the files

    Input
    -----
    raw_files: List[Tuple[Text, datetime.date, datetime.date]]

    Raises a RuntimeError if a date is missing.
    """
    first_date = min(raw_files, key=lambda f: f[1])[1]
    last_date = max(raw_files, key=lambda f: f[2])[2]

    dates_not_in_files = [
        date for date in daterange(first_date, last_date)
        if not any([f[1] <= date < f[2] for f in raw_files])
    ]

    if len(dates_not_in_files) > 0:
        err_msg = f"Error:\n  Dates\n"
        for d in dates_not_in_files:
            err_msg += f"    {d}\n"
        err_msg += "  not in any of the files."
        raise RuntimeError(err_msg)


def _tms_rawdata_dataframe_iterator(tms_num, raw_data_files):
    for fileinfo in raw_data_files:
        try:
            df = pd.read_hdf(fileinfo[0], key=f"tms_{tms_num}", mode='r')
            datetime_begin = datetime.datetime(year=fileinfo[1].year,
                                               month=fileinfo[1].month,
                                               day=fileinfo[1].day)
            datetime_end = datetime.datetime(year=fileinfo[2].year,
                                             month=fileinfo[2].month,
                                             day=fileinfo[2].day)
            yield (df, datetime_begin, datetime_end)
        except Exception as e:
            ...


_vehicle_categories = [1, 2, 3, 4, 5, 6, 7]
_directions = [1, 2]
_keys_default = np.array(
    [r for r in itertools.product(_vehicle_categories, _directions)])


def _aggregate_single_timeinterval(tms_num, t_begin, t_end, delta_t, df,
                                   file_Tbegin, file_Tend, rawdata_iterator):
    tb_in_file = file_Tbegin <= t_begin < file_Tend
    te_in_file = file_Tbegin <= t_end <= file_Tend
    if tb_in_file and te_in_file:  # The time interval is in the file
        vehicle_counts = [ df.loc[reduce(np.logical_and, (t_begin <= df['time'], df['time'] < t_end, df['direction'] == d, df['vehicle category'] == cat))].size
                for cat, d in _keys_default]

        keys = _keys_default
        begin_times = [t_begin] * len(keys)
        center_times = [t_begin + (t_end - t_begin) / 2] * len(keys)
        end_times = [t_end] * len(keys)
        res = pd.DataFrame({
            'beginning of time interval': begin_times,
            'time': center_times,
            'end of time interval': end_times,
            'direction': keys[:, 1],
            'vehicle category': keys[:, 0],
            'counts': vehicle_counts
        })
    elif tb_in_file and not te_in_file:  # Begin is in the file, but end is not.
        raise NotImplementedError()
    elif not tb_in_file and te_in_file:  # Time interval is wrong way around. Shouldn't happen.
        raise RuntimeError()
    else:  # Next file needed
        df, file_Tbegin, file_Tend = next(rawdata_iterator)
        res, file_Tbegin, file_Tend = _aggregate_single_timeinterval(tms_num, t_begin, t_end, delta_t,
                                            df, file_Tbegin, file_Tend,
                                            rawdata_iterator)
    return res, file_Tbegin, file_Tend


def _aggregate_core(tms_num, t_begin, t_end, delta_t,
                    raw_data_files) -> pd.DataFrame:

    # Iterator for the dataframes containing raw data for this particular
    # measuring station
    rawdata_iterator = _tms_rawdata_dataframe_iterator(tms_num, raw_data_files)
    # Iterator for all the timespans
    timespans = datetimerange(t_begin, t_end, delta_t)
    tms_aggregated_dfs = []
    try:
        df, file_Tbegin, file_Tend = next(rawdata_iterator)
    except StopIteration:
        return None  # TODO: Replace by zeroed aggregate dataframe
    for tb, te in timespans:
        _tmp_df, file_Tbegin, file_Tend = _aggregate_single_timeinterval(tms_num, tb, te, delta_t, df,
                                           file_Tbegin, file_Tend,
                                           rawdata_iterator)
        tms_aggregated_dfs.append(
            _tmp_df
            )
    tms_df = pd.concat(tms_aggregated_dfs)

    tms_df.to_hdf(
        f'aggregated_data/fin-traffic-{delta_t}-{t_begin}-{t_end}.h5',
        key=f'tms_{tms_num}',
        complevel=9,
        format='table',
        nan_rep='None'
    )
class Engine:

    def __init__(self, time0, time_end, delta_t,
                 raw_data_files):
        self.time0 = time0
        self.time_end = time_end
        self.delta_t = delta_t
        self.raw_data_files = raw_data_files

    def __call__(self, tms_num):
        _aggregate_core(tms_num, self.time0, self.time_end, self.delta_t,
                             self.raw_data_files)

def aggregate_datafiles(
        raw_data_files: List[Tuple[Text, datetime.date, datetime.date]],
        all_tms_numbers: List[int],
        delta_t: datetime.timedelta) -> pd.DataFrame:
    first_date = min(raw_data_files, key=lambda f: f[1])[1]
    last_date = max(raw_data_files, key=lambda f: f[2])[2]

    time0 = datetime.datetime(year=first_date.year,
                              month=first_date.month,
                              day=first_date.day,
                              hour=0,
                              minute=0)

    time_end = datetime.datetime(year=last_date.year,
                                 month=last_date.month,
                                 day=last_date.day,
                                 hour=0,
                                 minute=0)

    time = time0

    # Iterate over TMSs
    pool = multiprocessing.Pool()
    engine = Engine(time0, time_end, delta_t, raw_data_files)
    for _ in tqdm.tqdm(pool.imap(engine, all_tms_numbers)):
        pass
