from typing import Tuple, List, Optional, Union, Dict
import re
import matplotlib.pyplot as plt

import numpy as np
import requests
import folium
from pandas import DataFrame, Series
import pandas as pd
import os
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
import branca.colormap as cm


def add_space_before_ids(text: str) -> str:
    return re.sub(r'(?<=[A-Za-z]{2})([0-9]+)', ' \\1', text)


def replace_underscore_with_space(text: str) -> str:
    return text.replace('_', ' ')


filters = [
    add_space_before_ids,
    replace_underscore_with_space,
]


def apply_filters(s):
    for f in filters:
        s = f(s)
    return s


class WifiFeature:
    def __init__(
            self,
            data: DataFrame,
            max_features: Optional[int] = None,
            no_components: int = 30,
            random_state: int = 42,
            column_prefix: str = 'wifi_'
    ):
        self.__vectorizer = TfidfVectorizer(preprocessor=apply_filters, max_features=max_features)
        self.__svd = TruncatedSVD(n_components=no_components, random_state=random_state)
        self.__svd.fit(self.__vectorizer.fit_transform(data['SSID'].tolist()))
        self.__column_prefix = column_prefix

    def get_feature_names(self) -> List:
        return self.__vectorizer.get_feature_names()

    def transform(self, data: DataFrame, svd=True) -> DataFrame:
        grouped_data = data.groupby(by='epoch_time_id')['SSID'].apply(lambda x: ' '.join(x)).reset_index()
        if svd:
            return self.__to_sparse_dataframe(
                self.__svd.transform(self.__vectorizer.transform(grouped_data['SSID'].to_list())),
                list(range(self.__svd.n_components)),
                grouped_data['epoch_time_id'],
            )
        else:
            return self.__to_sparse_dataframe(
                self.__vectorizer.transform(grouped_data['SSID'].to_list()),
                self.__vectorizer.get_feature_names(),
                grouped_data['epoch_time_id'],
            )

    def __to_sparse_dataframe(
            self,
            data: Union[csr_matrix, np.ndarray],
            feature_names: List,
            index: Series,
            column_prefix: str = 'wifi_'
    ) -> DataFrame:
        columns = [column_prefix + str(feature_name) for feature_name in feature_names]
        if isinstance(data, csr_matrix):
            return DataFrame.sparse.from_spmatrix(
                data,
                columns=columns,
                index=index,
            )
        else:
            return DataFrame(
                data,
                columns=columns,
                index=index,
            )

    def hist(self, features_list: List):
        for i, features in enumerate(features_list):
            if isinstance(features, csr_matrix):
                plt.hist(features.indices)
            else:
                plt.hist(features[self.__column_prefix + str(i)])


class NoLocationFoundException(Exception):
    def __init__(self, message, cells):
        self.cells = cells
        self.message = message


GEOLOCATION_API_URL = 'https://www.googleapis.com/geolocation/v1/geolocate?key=' + os.getenv('GOOGLE_GEOLOCATION_API_KEY')
# GEOLOCATION_API_URL = 'https://backend.radiocells.org'


def fetch_location(cells: DataFrame) -> Dict:
    request_body = {
        "cellTowers": [{
            "cellId": int(cell.ci if not pd.isna(cell.ci) else cell.cid),
            "locationAreaCode": int(cell.TAC if not pd.isna(cell.ci) else cell.lac),
            "mobileCountryCode": int(cell.MCC),
            "mobileNetworkCode": int(cell.MNC)
        } for _, cell in cells.iterrows() if type(cell.ci) is not np.nan]
    }
    resp = requests.post(GEOLOCATION_API_URL, json=request_body)
    if resp.status_code == 200:
        # {"location": {"lat": 48.85702, "lng": 2.29520}, "accuracy": 30}
        location = resp.json()
        return {"Latitude": location['location']['lat'], "Longitude": location['location']['lng'], "accuracy": location['accuracy']}
    elif resp.status_code == 404:
        raise NoLocationFoundException('No location found', request_body)
    else:
        raise Exception(f'Service return {resp.status_code}')


def visualize_trace(data_location_with_label, markers=False):
    m = folium.Map(
        location=[data_location_with_label.iloc[1].Latitude, data_location_with_label.iloc[1].Longitude],
        zoom_start=10,
        tiles=None,
        control_scale=True,
    )

    folium.LatLngPopup().add_to(m)
    folium.raster_layers.TileLayer('OpenStreetMap', opacity=0.5).add_to(m)
    # folium.raster_layers.TileLayer('Stamen Toner',show=False, opacity=0.5).add_to(m)
    # folium.map.LayerControl().add_to(m)

    label_colors = {
        1: 'red',
        2: 'blue',
        3: 'green',
        4: 'purple',
        5: 'orange',
        6: 'darkred',
        7: 'violet',
        8: 'black',
        9: 'white',
    }

    colormap = cm.StepColormap(colors=label_colors.values(), vmax=max(label_colors.keys()), vmin=min(label_colors.keys()))
    colormap.caption = 'Still=1, Walking=2, Run=3, Bike=4, Car=5, Bus=6, Train=7, Subway=8'
    colormap.add_to(m)

    if not markers:
        folium.features.ColorLine(
            list(zip(data_location_with_label.Latitude.to_list(), data_location_with_label.Longitude.tolist())),
            colors=data_location_with_label.label.to_list(),
            colormap=colormap,
        ).add_to(m)
    else:
        for lat, lng in zip(data_location_with_label.Latitude.to_list(), data_location_with_label.Longitude.tolist()):
            folium.vector_layers.CircleMarker(location=(lat, lng), radius=3).add_to(m)


    return m