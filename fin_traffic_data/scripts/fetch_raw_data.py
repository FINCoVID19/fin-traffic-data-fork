import argparse
from datetime import date
import pathlib

import progressbar

from fin_traffic_data.metadata import *
from fin_traffic_data.raw_data import *
from fin_traffic_data.utils import daterange


def main():
    parser = argparse.ArgumentParser(
        description="Loads raw traffic data from Väylä")
    parser.add_argument(
        '--begin-date',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
        help='Date from which to begin data acquisition',
        required=True)
    parser.add_argument(
        '--end-date',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
        help='Day past the last day of the data.',
        required=True)

    args = parser.parse_args()
    tms_stations = get_tms_stations()

    municipality_id_name_map = get_municipalities()
    province_id_name_map = get_provinces()
    ely_id_name_map = get_ely_centers()
    ely_ids = list(ely_id_name_map.keys())

    bar = progressbar.ProgressBar(max_value=tms_stations.shape[0],
                                  redirect_stdout=True)
    pathlib.Path("raw_data/").mkdir(parents=True, exist_ok=True)
    for i, tms_station in tms_stations.iterrows():
        df = get_tms_raw_data(ely_ids, int(tms_station.num), args.begin_date,
                              args.end_date, False)
        if df is not None:
            df.to_hdf(
                f'raw_data/fin_traffic_raw_{args.begin_date}_{args.end_date}.h5',
                key = f"tms_{int(tms_station.num)}",
                complevel=9,
                format='table',
                nan_rep='None')


        bar.update(i)



if __name__ == '__main__':
    main()
