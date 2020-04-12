from fin_traffic_data.metadata import *
import pandas as pd
import numpy as np
from glob import glob
import networkx as nx
import matplotlib.pyplot as plt

G = nx.DiGraph()
_tmp = pd.read_hdf(adata_path, key='tms_3')
times = np.unique(_tmp['time'])
for _, row in maakunta_tms_data.iterrows():
    tms_infos = [tuple(v.split(',')) for v in row['tms'].split(';')]
    G.add_edge(row['source'], row['destination'], tms=tms_infos)
    df = None
    for tms_num, direction in tms_infos:
        _df = pd.read_hdf(adata_path, key=f'tms_{tms_num}').reset_index()
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
    df.to_hdf('tms_between_maakunnat.h5', key=f"{row['source']}:{row['destination']}", complevel=9, format='table')

nx.draw_networkx(G, pos=get_province_coordinates())
plt.show()
