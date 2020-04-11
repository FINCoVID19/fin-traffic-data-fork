from fin_traffic_data.metadata import *
import pandas as pd
import numpy as np
import networkx as nx
import unidecode
import matplotlib.pyplot as plt
erva_tms_data = pd.read_csv('tms_between_erva.csv')
adata_path = 'aggregated_data/fin-traffic-1:00:00-2020-01-01 00:00:00-2020-04-09 00:00:00.h5'

G = nx.DiGraph()
_tmp = pd.read_hdf(adata_path, key='tms_3')
times = np.unique(_tmp['time'])
for _, row in erva_tms_data.iterrows():
    tms_infos = [ tuple(v.split(',')) for v in row['tms'].split(';')]
    G.add_edge(row['source'], row['destination'], tms=tms_infos)
    df = None
    for tms_num, direction in tms_infos:
        _df = pd.read_hdf(adata_path,
                         key = f'tms_{tms_num}').reset_index()
        _df = _df.loc[_df['direction'] == int(direction)]

        if df is not None:
            for vehicle_category in [1,2,3,4,5,6,7]:
                    df.loc[df['vehicle category'] == vehicle_category, 'counts'] +=\
                        _df.loc[_df['vehicle category'] == vehicle_category]['counts'].values
        else:
            df = _df
    cols = df.columns.values.tolist()
    cols.remove('index')
    cols.remove('direction')
    df = df[cols]
    df.to_hdf('tms_between_ervas.h5',
              key = f"{row['source']}:{row['destination']}",
              complevel=9,
              format='table')

nx.draw_networkx(G, pos=get_catchment_area_coordinates())
plt.show()
