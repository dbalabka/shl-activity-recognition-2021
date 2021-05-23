from typing import Tuple, List, Optional, Union
import re
import matplotlib.pyplot as plt

import numpy as np
from pandas import DataFrame, Series
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer


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
