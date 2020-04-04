import datetime
from io import StringIO
from typing import List

import requests
import pandas as pd
import numpy as np
import progressbar

from fin_traffic_data.utils import daterange

_tms_raw_column_names = [
    'tms_id', 'year', 'day_number', 'hour', 'minute', 'second', 'millisecond',
    'length', 'lane', 'direction', 'vehicle category', 'speed', 'faulty',
    'total time', 'timespan', 'queue_begin'
]


def _tms_raw_date_parser(*args) -> np.ndarray:
    """Datetime parser for TMS raw data"""
    years, days, hours, minutes, seconds, milliseconds = args
    years = years.astype(int) + 2000
    days = days.astype(int)
    hours = hours.astype(int)
    minutes = minutes.astype(int)
    seconds = seconds.astype(int)
    milliseconds.astype(int)
    d0 = datetime.datetime(years[0], 1, 1)
    return np.array([
        d0 + datetime.timedelta(days=int(days[i]),
                                hours=int(hours[i]),
                                minutes=int(minutes[i]),
                                seconds=int(seconds[i]),
                                milliseconds=int(milliseconds[i]))
        for i in range(years.size)
    ])


def get_tms_raw_data(ely_id_list: List[int],
                     tms_id: int,
                     date_begin: datetime.date,
                     date_end: datetime.date,
                     show_progress=True) -> pd.DataFrame:
    """
    Fetches raw TMS data from a single TMS location between
    dates date_begin and date_end.

    Input
    -----
    ely_id : List[int]
        List of all IDs of ELY centers
    tms_id : int
        ID of the TMS
    date_begin: datetime.date
        First date of the range
    date_end: datetime.date
        Last date of the range (exclusive)
    show_progress: bool (optional)
        Whether to show progressbar

    Returns
    -------
    pandas.DataFrame with one row for each recorded vehicle. Columns are
        - tms_id
        - time
        - direction
        - vehicle category

    or None if the TMS cannot be found in any ELY center's dataset (likely an old TMS id).
    """
    ely_ids = ["%02d" % eid for eid in ely_id_list]

    tms_id = int(tms_id)

    ely_id = None
    dfs = []
    if show_progress:
        bar = progressbar.ProgressBar(
            max_value=int((date_end - date_begin).days - 1))
    for i, date in enumerate(daterange(date_begin, date_end)):

        year_date0 = datetime.date(date.year, 1, 1)
        day_number = (date - year_date0).days + 1
        year_short = f"{date:%y}"
        try:
            if not ely_id:
                for possible_ely_id in ely_ids:
                    resp = requests.get(
                        'https://aineistot.vayla.fi/lam/rawdata/' +
                        f'{date.year}/{possible_ely_id}/lamraw_{tms_id}_{year_short}_{day_number}.csv'
                    )
                    if resp.status_code == 200:
                        ely_id = possible_ely_id
                        break
            if not ely_id:
                raise RuntimeError("No ELY center has this TMS")
            resp = requests.get(
                'https://aineistot.vayla.fi/lam/rawdata/' +
                f'{date.year}/{ely_id}/lamraw_{tms_id}_{year_short}' +
                f'_{day_number}.csv')
            if resp.status_code != 200:
                raise RuntimeError
            stream = StringIO(resp.text)
            stream.seek(0)
            df = pd.read_csv(stream,
                             names=_tms_raw_column_names,
                             delimiter=';',
                             usecols=_tms_raw_column_names[:-3],
                             parse_dates={
                                 'time': [
                                     'year', 'day_number', 'hour', 'minute',
                                     'second', 'millisecond'
                                 ]
                             },
                             date_parser=_tms_raw_date_parser)
            # Remove faulty readings and empty datasets
            if 'time' in df.columns:
                df = df.loc[df['faulty'] == 0][[
                    'tms_id', 'time', 'direction', 'vehicle category'
                ]]
                dfs.append(df)
        except RuntimeError as e:
            print(f"Could not load {tms_id}, {date}")
            if not ely_id:
                return None
        except Exception as e:
            import pdb; pdb.set_trace()
            print("Unknown error")
        finally:
            if show_progress:
                bar.update(i)
    if dfs:
        return pd.concat(dfs)
    else:
        return None
