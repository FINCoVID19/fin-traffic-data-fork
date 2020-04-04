import argparse
from datetime import date

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

    all_dfs = []
    for i, tms_station in tms_stations.iterrows():
        df = get_tms_raw_data(ely_ids, int(tms_station.num), args.begin_date,
                              args.end_date, False)
        if df is not None:
            all_dfs.append(df)

        bar.update(i)

    df_full = pd.concat(all_dfs)

    df_full.to_hdf(f'fi_trafi_raw_{args.begin_date}_{args.end_date}.pd.h5',
                   key='dataframe')

    tms_stations.to_hdf(
        f'fi_trafi_raw_{args.begin_date}_{args.end_date}.pd.h5',
        key='tms_stations')


if __name__ == '__main__':
    main()
