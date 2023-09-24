# Creating Folium DataSet
# ref: https://youtu.be/DsInj1FmMmQ?si=R8EAZmRLW_CZIyKd

from typing import Dict, AnyStr
from kedro.io import AbstractDataSet, DataSetError
from folium import Map


class FoliumHTMLDataSet(AbstractDataSet):
    def __init__(self, filepath: str):
        self._filepath = filepath

    def _load(self) -> None:
        raise DataSetError('This DataSet is WriteOnly')

    def _describe(self) -> Dict[str, AnyStr]:
        return dict(filepath=self._filepath)

    def _save(self, data: Map) -> None:
        data.save(self._filepath)
