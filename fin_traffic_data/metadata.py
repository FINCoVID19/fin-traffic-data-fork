import requests
import pandas as pd
from typing import Dict, Text


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

    tms_ids = [tms['id'] for tms in data]
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
    resp = requests.get('https://tie.digitraffic.fi/api/v3/metadata/locations')

    data = resp.json()

    iters = filter(
        lambda item: item['properties']['subtypeCode'].startswith('A9.'),
        data['features'])
    municipality_id_name_map = dict(
        sorted([(int(it['id']), it['properties']['firstName'])
                for it in iters]))
    return municipality_id_name_map


def get_provinces() -> Dict[int, Text]:
    """
    Returns a mapping between province id and its name.

    Returns
    -------
    Dict
    """
    resp = requests.get('https://tie.digitraffic.fi/api/v3/metadata/locations')

    data = resp.json()

    iters = filter(
        lambda item: item['properties']['subtypeCode'].startswith('A8.'),
        data['features'])
    province_id_name_map = dict(
        sorted([(int(it['id']), it['properties']['firstName'])
                for it in iters]))
    return province_id_name_map
