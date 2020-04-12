import argparse
from glob import glob

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

from fin_traffic_data.metadata import *


def main():

    parser = argparse.ArgumentParser(
        description="Collects and aggregates the aggregated TMS data for traffic between provinces."
    )

    parser.add_argument(
        "--input", required=True, help="Path to the aggregated_data input-file"
    )
    parser.add_argument(
        "--visualize", action='store_true', help="Visualizes the province graph."
    )

    args = parser.parse_args()
    inputfile = args.input
    visualization_enabled = args.visualize

    area='province'
    tms_over_area_borders = get_tms_over_province_borders()

    # Instantiate the directed graph
    G = nx.DiGraph()

    # Read
    _tmp = pd.read_hdf(inputfile, key='tms_3')
    times = np.unique(_tmp['time'])
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
        df.to_hdf('tms_between_provinces.h5', key=f"{row['source']}:{row['destination']}", complevel=9, format='table')

    # Create map of areaName -> (longitude, latitude)
    if area=='province':
        data = get_province_info()
    elif area=='erva':
        data = get_erva_info()
    coordinate_map = dict([(i[0], i[1:]) for i in data[[area, 'longitude', 'latitude']].values.tolist()])

    nx.draw_networkx(G, pos=coordinate_map)
    plt.show()

if __name__ == '__main__':
    main()
