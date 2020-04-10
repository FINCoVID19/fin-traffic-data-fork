import requests
import pandas as pd
from typing import Dict, Text, Tuple
import numpy as np

from fin_traffic_data.municipality_neighbours import municipality_neighbours

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
    resp = requests.get(
        'https://tie.digitraffic.fi/api/v3/metadata/tms-stations')

    data = resp.json()

    unique_elems = set([(int(i['properties']['municipalityCode']),
                         i['properties']['municipality'])
                        for i in data['features']])
    return dict(unique_elems)


def get_municipality_to_province_map() -> Dict[int, Text]:
    """
    Returns a mapping between municipality id and its name.

    Returns
    -------
    Dict
    """
    resp = requests.get(
        'https://tie.digitraffic.fi/api/v3/metadata/tms-stations')

    data = resp.json()

    unique_elems = set([(int(i['properties']['municipalityCode']),
                         int(i['properties']['provinceCode']))
                        for i in data['features']])
    return dict(unique_elems)


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
        'Päihät-Häme': [25.6612, 60.9827],
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
        'Lappi': [25.7294, 66.5039]
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

if __name__=='__main__':
    municipalities = get_municipalities()
    un = []
    neighb_mun = get_neighbouring_municipalities_map()
    for key, val in neighb_mun.items():
        for k in val:
            if k not in neighb_mun.keys():
                un.append(k)
    print(sorted(set(un)))
