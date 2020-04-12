import argparse
from enum import Enum, unique
from glob import glob

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

from fin_traffic_data.metadata import *


@unique
class Area(Enum):
    PROVINCE = "province"
    ERVA = "erva"

    def __str__(self):
        if self is Area.PROVINCE:
            return "province"
        else:
            return "erva"


def main():

    parser = argparse.ArgumentParser(
        description="Collects and aggregates the aggregated TMS data for traffic between provinces or ERVAs."
    )

    parser.add_argument("--input", required=True, help="Path to the aggregated_data input-file")
    parser.add_argument("--visualize", action='store_true', help="Visualizes the province/ERVA graph.")
    parser.add_argument("--area", type=Area, required=True, help="Whether to collect erva or province-level data.")

    args = parser.parse_args()
    inputfile = args.input
    visualization_enabled = args.visualize

    # Select ERVA / province
    area = args.area
    if area is Area.PROVINCE:
        tms_over_area_borders = get_tms_over_province_borders()
    elif area is Area.ERVA:
        tms_over_area_borders = get_tms_over_erva_borders()

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
        df.to_hdf(f'tms_between_{area}s.h5', key=f"{row['source']}:{row['destination']}", complevel=9, format='table')

    # Create map of areaName -> (longitude, latitude)
    if area == Area.PROVINCE:
        data = get_province_info()
        coordinate_map = dict([(row['province'], row[['longitude', 'latitude']]) for key, row in data.iterrows()])
    elif area == Area.ERVA:
        data = get_erva_info()
        coordinate_map = dict([(key, row[['longitude', 'latitude']]) for key, row in data.iterrows()])

    nx.draw_networkx(G, pos=coordinate_map)
    plt.show()


if __name__ == '__main__':
    main()
