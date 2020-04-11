import pickle
import requests
import pandas as pd
from typing import Dict, Text, Tuple
import numpy as np
import os.path
from fin_traffic_data.municipality_neighbours import municipality_neighbours
import fin_traffic_data


def get_tms_stations() -> pd.DataFrame:
    """
    Obtain a list of TMS stations and their properties.

    Returns
    -------
    pandas.DataFrame with columns
        - id : TMS station id
        - coords_x : x-coordinate of the station
        - coords_y : y-coordinate of the station
        - municipality : integer code of the municipality
        - province : integer code of the province
        - dir1 : integer code of the direction 1 municipality
        - dir2 : integer code of the direction 2 municipality
    """
    resp = requests.get(
        'https://tie.digitraffic.fi/api/v3/metadata/tms-stations')
    data = resp.json()['features']

    tms_ids = [int(tms['id']) for tms in data]
    tms_nums = [tms['properties']['tmsNumber'] for tms in data]
    tms_coords_x = [tms['geometry']['coordinates'][0] for tms in data]
    tms_coords_y = [tms['geometry']['coordinates'][1] for tms in data]
    tms_municipality_codes = [
        int(tms['properties']['municipalityCode']) for tms in data
    ]
    tms_province_codes = [
        int(tms['properties']['provinceCode']) for tms in data
    ]
    tms_dir1_municipality_codes = [
        tms['properties']['direction1MunicipalityCode'] for tms in data
    ]
    tms_dir2_municipality_codes = [
        tms['properties']['direction2MunicipalityCode'] for tms in data
    ]

    df = pd.DataFrame({
        'id': tms_ids,
        'num': tms_nums,
        'coords_x': tms_coords_x,
        'coords_y': tms_coords_y,
        'municipality': tms_municipality_codes,
        'province': tms_province_codes,
        'dir1': tms_dir1_municipality_codes,
        'dir2': tms_dir2_municipality_codes
    })
    return df


def get_municipalities() -> Dict[int, Text]:
    """
    Returns a mapping between municipality id and its name.

    Returns
    -------
    Dict
    """
    df = pd.read_csv(os.path.dirname(fin_traffic_data.__file__) +
                     '/data/kunnat2020.csv',
                     delimiter=',')
    return dict(
        [ (int(row['num'].replace("'","")), row['name']) for _,row in df.iterrows()]),\
            dict(
                [ (row['name'], [row['coords_x'], row['coords_y']]) for _,row in df.iterrows()])


def get_municipality_to_province_map() -> Dict[int, Text]:
    """
    Returns a mapping between municipality id and its name.

    Returns
    -------
    Dict
    """
    df = pd.read_csv(os.path.dirname(fin_traffic_data.__file__) +
                     '/data/kunta2maakunta.txt',
                     delimiter=';')
    return dict(df[['kunta', 'maakunta']].values.tolist())


def get_provinces() -> Dict[int, Text]:
    """
    Returns a mapping between province id and its name.

    Returns
    -------
    Dict
    """
    resp = requests.get(
        'https://tie.digitraffic.fi/api/v3/metadata/tms-stations')

    data = resp.json()

    unique_elems = set([(int(i['properties']['provinceCode']),
                         i['properties']['province'])
                        for i in data['features']])
    return dict(unique_elems)


def get_ely_centers() -> Dict[int, Text]:
    return {
        1: "Uusimaa, Häme",
        2: "Varsinais-Suomi, Satakunta",
        3: "Kaakkois-Suomi",
        4: "Pirkanmaa",
        8: "Pohjois-Savo, Etelä-Savo, Pohjois-Karjala",
        9: "Keski-Suomi",
        10: "Etelä-Pohjanmaa, Pohjanmaa",
        12: "Pohjois-Pohjanmaa, Kainuu",
        14: "Lappi"
    }


def get_province_coordinates() -> Dict[Text, Tuple[float, float]]:
    coordinate_data = {
        'Uusimaa': [24.9384, 60.1699],
        'Varsinais-Suomi': [22.2666, 60.45189],
        'Ahvenanmaa': [19.9348, 60.0971],
        'Kanta-Häme': [24.4590, 60.9929],
        'Päijät-Häme': [25.6612, 60.9827],
        'Kymenlaakso': [26.7042, 60.8679],
        'Etelä-Karjala': [28.1897, 61.0550],
        'Etelä-Savo': [27.2721, 61.6887],
        'Keski-Suomi': [25.7473, 62.2426],
        'Pirkanmaa': [23.7610, 61.4978],
        'Satakunta': [21.7974, 61.4851],
        'Pohjanmaa': [21.6165, 63.0951],
        'Etelä-Pohjanmaa': [22.8504, 62.7877],
        'Keski-Pohjanmaa': [23.1250, 63.8415],
        'Pohjois-Savo': [27.6782, 62.8980],
        'Pohjois-Karjala': [29.7636, 62.6010],
        'Kainuu': [27.7278, 64.2222],
        'Pohjois-Pohjanmaa': [25.4651, 65.0121],
        'Lappi': [25.7294, 66.5039],
        'Norja': [24.200713, 69.647095],
        'Ruotsi': [18.283758, 65.691331],
        'Venäjä': [35.411563, 65.140340]
    }

    return coordinate_data


def get_province_to_laani_map():
    return {
        'Varsinais-Suomi': 'Turun ja Porin lääni',
        'Satakunta': 'Turun ja Porin lääni',
        'Uusimaa': 'Uudenmaan lääni',
        'Kanta-Häme': 'Hämeen lääni',
        'Pirkanmaa': 'Hämeen lääni',
        'Päijät-Häme': 'Mikkelin lääni',
        'Kymenlaakso': 'Kymen lääni',
        'Etelä-Karjala': 'Kymen lääni',
        'Etelä-Savo': 'Mikkelin lääni',
        'Pohjois-Karjala': 'Pohjois-Karjalan lääni',
        'Pohjois-Savo': 'Kuopion lääni',
        'Keski-Suomi': 'Keski-Suomen lääni',
        'Pohjanmaa': 'Vaasan lääni',
        'Etelä-Pohjanmaa': 'Vaasan lääni',
        'Keski-Pohjanmaa': 'Vaasan lääni',
        'Pohjois-Pohjanmaa': 'Oulun lääni',
        'Kainuu': 'Oulun lääni',
        'Lappi': 'Lapin lääni'
    }


def get_laani_coordinates():
    return {
        'Turun ja Porin lääni': [21.7974, 61.4851],
        'Uudenmaan lääni': [24.9384, 60.1699],
        'Oulun lääni': [25.4651, 65.0121],
        'Mikkelin lääni': [27.2721, 61.6887],
        'Vaasan lääni': [21.6165, 63.0951],
        'Kuopion lääni': [27.6782, 62.8980],
        'Kymen lääni': [28.1897, 61.0550],
        'Hämeen lääni': [23.7610, 61.4978],
        'Ahvenanaan lääni': [19.9348, 60.0971],
        'Lapin lääni': [25.7294, 66.5039],
        'Keski-Suomen lääni': [25.7473, 62.2426],
        'Pohjois-Karjalan lääni': [29.7636, 62.6010]
    }


def get_neighbouring_municipalities_map():
    return municipality_neighbours


if __name__ == '__main__':
    import networkx as nx
    import networkx.drawing.nx_pylab as nxd
    from networkx.algorithms import *
    import matplotlib.pyplot as plt
    tms_stations = get_tms_stations()
    municipality_nums_with_tms = list(
        set(tms_stations['municipality'].values.tolist()))
    municipalities, municipalities_coords_map = get_municipalities()
    municipality_neighbours = get_neighbouring_municipalities_map()

    G = nx.Graph()

    # Add all municipalities and connect them to their
    # geographic neighbours
    for mun_num, name in municipalities.items():
        G.add_node(name)
        neighbs = municipality_neighbours[name]
        for n2 in neighbs:
            G.add_edge(name, n2)

    # Graph of all municipalities with TMS
    # and their connections. Edges with
    # TMS are annotated with their info
    Gtms = nx.DiGraph()
    # Add all municipalities that have TMS
    for mun_num in tms_stations['municipality'].values:
        name = municipalities[mun_num]
        Gtms.add_node(name)

    # For all TMSs, add all municipalities
    # from the path between the TMS
    # and direction municipalities
    for _, row in tms_stations.iterrows():
        tms_num = row['num']
        name = municipalities[row['municipality']]
        try:
            dir1_num = int(row['dir1'])
        except:
            dir1_num = None
        try:
            dir2_num = int(row['dir2'])
        except:
            dir2_num = None

        if dir1_num:
            try:
                dir1 = municipalities[dir1_num]
                path1 = bidirectional_shortest_path(G, name, dir1)
                for i in range(len(path1) - 1):
                    if i == 0:
                        data = Gtms.get_edge_data(path1[i],
                                                  path1[i + 1],
                                                  default=None)
                        if data:
                            data['tms'] = data['tms'] + [int(tms_num)]
                            data['tms_dir'] = data['tms_dir'] + [1]
                        else:
                            data = {'tms': [int(tms_num)], 'tms_dir': [1]}
                        Gtms.add_edge(path1[i], path1[i + 1], **data)
                    else:
                        Gtms.add_edge(path1[i], path1[i + 1])
            except:
                print(f"Failed {name}: dir1, {dir1_num}")
        if dir2_num:
            try:
                dir2 = municipalities[dir2_num]
                path2 = bidirectional_shortest_path(G, name, dir2)
                for i in range(len(path2) - 1):
                    if i == 0:
                        data = Gtms.get_edge_data(path2[i],
                                                  path2[i + 1],
                                                  default=None)
                        if data:
                            data['tms'] = data['tms'] + [int(tms_num)]
                            data['tms_dir'] = data['tms_dir'] + [2]
                        else:
                            data = {'tms': [int(tms_num)], 'tms_dir': [2]}
                        Gtms.add_edge(path2[i], path2[i + 1], **data)
                    else:
                        data = Gtms.get_edge_data(path2[i],
                                                  path2[i + 1],
                                                  default=None)
                        if not data:
                            Gtms.add_edge(path2[i], path2[i + 1])
            except:
                print(f"Failed {name}: dir2, {dir2_num}")

    #
    Gprovinces = nx.DiGraph()

    province_name_to_coords_map = get_province_coordinates()
    for key in province_name_to_coords_map.keys():
        Gprovinces.add_node(key)

    m2p = get_municipality_to_province_map()
    for source, dest, mun_data in Gtms.edges(data=True):
        if mun_data:
            source_province = m2p[source]
            dest_province = m2p[dest]
            if source_province != dest_province:
                data = Gprovinces.get_edge_data(source_province,
                                                dest_province,
                                                default=None)
                if data:
                    data['tms'] = data['tms'] + mun_data['tms']
                    data['tms_dir'] = data['tms_dir'] + mun_data['tms_dir']
                else:
                    data = mun_data
                Gprovinces.add_edge(source_province, dest_province, **data)

    to_delete = list(nx.isolates(Gprovinces))
    Gprovinces.remove_nodes_from(to_delete)

    province_connections = dict([((edge[0], edge[1]), {
        'tms': edge[2]['tms'],
        'direction': edge[2]['tms_dir']
    }) for edge in Gprovinces.edges(data=True)])
    with open('maakunta_crossings.pkl', 'wb') as f:
        pickle.dump(province_connections, f)


