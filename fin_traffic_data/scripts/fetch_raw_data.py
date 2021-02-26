import argparse
import datetime
import pathlib
import progressbar
import os
from fin_traffic_data.metadata import get_tms_stations, get_province_info
from fin_traffic_data.raw_data import get_tms_raw_data


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

    parser.add_argument("--results_dir", "-rd",
                        type=str,
                        default='raw_data',
                        help="Name of the directory to store the results.")

    args = parser.parse_args()

    # Get info on available TMS stations
    tms_stations = get_tms_stations()

    # Get info on provinces
    province_info = get_province_info()

    if args.progressbar:
        bar = progressbar.ProgressBar(max_value=tms_stations.shape[0], redirect_stdout=True)

    # Create the output directory
    pathlib.Path(args.results_dir).mkdir(parents=True, exist_ok=True)

    # Load data for each TMS
    it = 0
    tms_stations_list = tms_stations.values.tolist()
    for tms_station in tms_stations_list:
        province = int(tms_station[5])
        num = int(tms_station[1])
        ely_id = province_info.loc[province]['ely-center (traffic)']
        df = get_tms_raw_data(ely_id, num, args.begin_date, args.end_date, False)
        if df is not None:
            file_name = 'fin_traffic_raw_%s_%s.h5' % (args.begin_date,
                                                      args.end_date)
            result_path = os.path.join(args.results_dir, file_name)
            df.to_hdf(
                result_path,
                mode='a',
                key=f"tms_{num}",
                format='fixed',
                nan_rep='None',
                complib="blosc:snappy"
            )
        if args.progressbar:
            it += 1
            bar.update(it)


if __name__ == '__main__':
    main()
