import json
import requests
import pandas as pd
from typing import Dict, Text, Tuple
import numpy as np
import os.path


def get_tms_stations() -> pd.DataFrame:
    """
    Obtain a list of TMS stations and their properties.

    Returns
    -------
    pandas.DataFrame with columns
        - id : TMS station id
        - latitude : x-coordinate of the station
        - longitude : y-coordinate of the station
        - municipality : integer code of the municipality
        - province : integer code of the province
        - dir1 : integer code of the direction 1 municipality
        - dir2 : integer code of the direction 2 municipality
    """
    resp = requests.get('https://tie.digitraffic.fi/api/v3/metadata/tms-stations')
    data = resp.json()['features']

    tms_ids = [int(tms['id']) for tms in data]
    tms_nums = [tms['properties']['tmsNumber'] for tms in data]
    tms_latitude = [tms['geometry']['coordinates'][0] for tms in data]
    tms_longitude = [tms['geometry']['coordinates'][1] for tms in data]
    tms_municipality_codes = [int(tms['properties']['municipalityCode']) for tms in data]
    tms_province_codes = [int(tms['properties']['provinceCode']) for tms in data]
    tms_dir1_municipality_codes = [tms['properties']['direction1MunicipalityCode'] for tms in data]
    tms_dir2_municipality_codes = [tms['properties']['direction2MunicipalityCode'] for tms in data]

    df = pd.DataFrame(
        {
            'id': tms_ids,
            'num': tms_nums,
            'latitude': tms_latitude,
            'longitude': tms_longitude,
            'municipality': tms_municipality_codes,
            'province': tms_province_codes,
            'dir1': tms_dir1_municipality_codes,
            'dir2': tms_dir2_municipality_codes
        },
         index= tms_nums)
    return df


def get_municipality_info() -> pd.DataFrame:
    """
    Returns the following information on every municipality:
        - it's municipalityNumber
        - name
        - province it belongs to
        - provinceNumber
        - latitude
        - longitude

    Returns
    -------
    pandas.DataFrame
    """
    df = pd.read_csv(os.path.dirname(__file__) + '/data/municipality_info.csv', index_col=[0])
    return df


def get_province_info() -> pd.DataFrame:
    """
    Returns the following information on every province:
        - it's provinceNumber
        - name
        - latitude
        - longitude
        - erva it belongs to (university hospital catchment area)
        - ely-center under which the province's traffic management is

    Returns
    -------
    pandas.DataFrame
    """
    df = pd.read_csv(os.path.dirname(__file__) + '/data/province_info.csv', index_col=[0])
    return df


def get_erva_info() -> pd.DataFrame:
    """
    Returns the coordinates of each erva

    Returns
    -------
    pandas.DataFrame
    """
    df = pd.read_csv(os.path.dirname(__file__) + '/data/erva_coordinates.csv', index_col=[0])
    return df


def get_neighbouring_municipalities_map() -> Dict:
    """
    Returns a map from municipality name to a list of its neighbours.

    Returns
    -------
    Dict
    """
    neighbour_map = json.load(open(os.path.dirname(__file__) + '/data/province_info.csv', 'r'))
    return neighbour_map


def get_tms_over_province_borders() -> pd.DataFrame:
    """
    Returns a dataframe of TMS stations and the measurement directions
    between provinces.
    """
    df = pd.read_csv(os.path.dirname(__file__) + '/data/tms_over_province_borders.csv')
    return df


def get_tms_over_erva_borders() -> pd.DataFrame:
    """
    Returns a dataframe of TMS stations and the measurement directions
    between ERVAs.
    """
    df = pd.read_csv(os.path.dirname(__file__) + '/data/tms_over_erva_borders.csv')
    return df
