import os
import sys
import pathlib
import argparse
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

from fin_traffic_data.metadata import (
    get_tms_over_province_borders, get_tms_over_erva_borders,
    get_tms_over_hcd_borders, get_province_info, get_erva_info,
    get_hcd_info
)


def get_aggregated_traffic_between_areas(inputfile, area,
                                         visualization_enabled, results_dir):
    # Select ERVA / province
    if area == 'province':
        tms_over_area_borders = get_tms_over_province_borders()
    elif area == 'erva':
        tms_over_area_borders = get_tms_over_erva_borders()
    elif area == 'hcd':
        tms_over_area_borders = get_tms_over_hcd_borders()

    # Create the output directory
    pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

    # Instantiate the directed graph
    G = nx.DiGraph()

    # Read
    for _, row in tms_over_area_borders.iterrows():
        tms_infos = [tuple(v.split(',')) for v in row['tms'].split(';')]
        G.add_edge(row['source'], row['destination'], tms=tms_infos)
        df = None
        for tms_num, direction in tms_infos:
            _df = pd.read_hdf(inputfile, key=f'tms_{tms_num}').reset_index()
            _df = _df.loc[_df['direction'] == int(direction)]

            if df is not None:
                for vehicle_category in [1, 2, 3, 4, 5, 6, 7]:
                    df.loc[df['vehicle category'] == vehicle_category, 'counts'] +=\
                        _df.loc[_df['vehicle category'] == vehicle_category]['counts'].values
            else:
                df = _df
        cols = df.columns.values.tolist()
        cols.remove('index')
        cols.remove('direction')
        df = df[cols]
        input_filename = inputfile.split('/')[-1]
        file_name = 'tms_between_%ss_input_%s.h5' % (area,
                                                     input_filename.split('.')[0])
        result_path = os.path.join(results_dir, file_name)
        df.to_hdf(result_path,
                  key=f"{row['source']}:{row['destination']}",
                  complevel=9,
                  format='table')

    # Create map of areaName -> (longitude, latitude)
    if area == 'province':
        data = get_province_info()
        coordinate_map = dict([(row['province'], row[['longitude', 'latitude']]) for key, row in data.iterrows()])
    elif area == 'erva':
        data = get_erva_info()
        coordinate_map = dict([(key, row[['longitude', 'latitude']]) for key, row in data.iterrows()])
    elif area == 'hcd':
        data = get_hcd_info()
        coordinate_map = dict([(key, row[['longitude', 'latitude']]) for key, row in data.iterrows()])

    if(visualization_enabled):
        nx.draw_networkx(G, pos=coordinate_map)
        plt.show()

    return result_path


# Parse script arguments
def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description="Collects and aggregates the aggregated TMS data for traffic between provinces or ERVAs."
    )

    parser.add_argument("--input",
                        required=True,
                        help="Path to the aggregated_data input-file")
    parser.add_argument("--visualize",
                        action='store_true',
                        help="Visualizes the province/ERVA graph.")
    parser.add_argument("--area",
                        type=str,
                        default="hcd",
                        choices=["province", "erva", "hcd"],
                        help="Whether to collect erva or province or hcd-level data.")
    parser.add_argument("--results_dir", "-rd",
                        type=str,
                        default='aggregated_data_area',
                        help="Name of the directory to store the results.")

    return parser.parse_args(args)


def main():
    args = parse_args()
    get_aggregated_traffic_between_areas(inputfile=args.input,
                                         area=args.area,
                                         visualization_enabled=args.visualize,
                                         results_dir=args.results_dir)


if __name__ == '__main__':
    main()
