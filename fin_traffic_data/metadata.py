import requests
import pandas as pd
from typing import Dict, Text
import numpy as np

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
    tms_nums = [ tms['properties']['tmsNumber'] for tms in data]
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
    resp = requests.get('https://tie.digitraffic.fi/api/v3/metadata/tms-stations')

    data = resp.json()

    unique_elems = set([ (int(i['properties']['municipalityCode']), i['properties']['municipality']) for i in data['features']])
    return dict(unique_elems)


def get_provinces() -> Dict[int, Text]:
    """
    Returns a mapping between province id and its name.

    Returns
    -------
    Dict
    """
    resp = requests.get('https://tie.digitraffic.fi/api/v3/metadata/tms-stations')

    data = resp.json()

    unique_elems = set([ (int(i['properties']['provinceCode']), i['properties']['province']) for i in data['features']])
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
