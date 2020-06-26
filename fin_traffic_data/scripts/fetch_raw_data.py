import argparse
from datetime import date
import pathlib

import progressbar

from fin_traffic_data.metadata import *
from fin_traffic_data.raw_data import *
from fin_traffic_data.utils import daterange


def main():
    parser = argparse.ArgumentParser(description="Loads raw traffic data from Väylä")
    parser.add_argument(
        '--begin-date',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
        help='Date from which to begin data acquisition',
        required=True
    )
    parser.add_argument(
        '--end-date',
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
        help='Day past the last day of the data.',
        required=True
    )
    parser.add_argument(
        '--progressbar',
        action='store_true',
        default=False,
        help='Shows progressbar',
        required=False)

    args = parser.parse_args()

    # Get info on available TMS stations
    tms_stations = get_tms_stations()

    # Get info on provinces
    province_info = get_province_info()

    # Get ids of ely-centers responsible for traffic management
    ely_ids = np.unique(province_info['ely-center (traffic)'].values)

    if args.progressbar:
        bar = progressbar.ProgressBar(max_value=tms_stations.shape[0], redirect_stdout=True)

    # Create the output directory
    pathlib.Path("raw_data/").mkdir(parents=True, exist_ok=True)

    # Load data for each TMS
    it = 0
    for i, tms_station in tms_stations.iterrows():
        ely_id = province_info.loc[int(tms_station.province)]['ely-center (traffic)']
        df = get_tms_raw_data(ely_id, int(tms_station.num), args.begin_date, args.end_date, False)
        if df is not None:
            df.to_hdf(
                f'raw_data/fin_traffic_raw_{args.begin_date}_{args.end_date}.h5',
                mode='a',
                key=f"tms_{int(tms_station.num)}",
                format='fixed',
                nan_rep='None',
                complib="blosc:snappy"
            )
        if args.progressbar:
            it += 1
            bar.update(it)


if __name__ == '__main__':
    main()
