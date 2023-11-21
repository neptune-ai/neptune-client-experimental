#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
__all__ = [
    "which_fetchable",
    "Fetchable",
    "FetchableAtom",
    "FetchableSet",
    "FetchableSeries",
    "Downloadable",
    "DownloadableFile",
    "DownloadableSet",
]

import typing
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Union,
)

from neptune.common.warnings import NeptuneUnsupportedType
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.utils import verify_type

from neptune_fetcher.attributes import (
    Attr,
    Boolean,
    Datetime,
    Float,
    FloatSeries,
    Integer,
    Series,
    Set,
    String,
    StringSeries,
    StringSet,
)

if TYPE_CHECKING:
    from pandas import DataFrame

    from neptune_fetcher.custom_backend import CustomBackend


ATOMS = {
    AttributeType.INT,
    AttributeType.FLOAT,
    AttributeType.BOOL,
    AttributeType.DATETIME,
    AttributeType.STRING,
    AttributeType.GIT_REF,
    AttributeType.RUN_STATE,
    AttributeType.ARTIFACT,
}
SERIES = {AttributeType.FLOAT_SERIES, AttributeType.STRING_SERIES}


class Fetchable(ABC):
    def __init__(
        self,
        attribute: Attribute,
        backend: "CustomBackend",
        container_id: str,
        cache: Dict[str, Union[Attr, Series, Set]],
    ) -> None:
        self._attribute = attribute
        self._backend = backend
        self._container_id = container_id
        self._cache = cache

    @abstractmethod
    def fetch(self):
        ...


class FetchableAtom(Fetchable):
    def fetch(self):
        if self._attribute.path in self._cache:
            # from cache
            return self._cache[self._attribute.path].val
        if self._attribute.type == AttributeType.STRING:
            attr = String(self._attribute.type)
        elif self._attribute.type == AttributeType.INT:
            attr = Integer(self._attribute.type)
        elif self._attribute.type == AttributeType.BOOL:
            attr = Boolean(self._attribute.type)
        elif self._attribute.type == AttributeType.DATETIME:
            attr = Datetime(self._attribute.type)
        elif self._attribute.type == AttributeType.FLOAT:
            attr = Float(self._attribute.type)
        else:
            raise NeptuneUnsupportedType()
        attr.val = attr.fetch(self._backend, self._container_id, ContainerType.RUN, [self._attribute.path])
        self._cache[self._attribute.path] = attr
        return attr.val


class FetchableSeries(Fetchable):
    def fetch(self):
        raise NeptuneUnsupportedType()

    def fetch_values(self, *, include_timestamp: bool = True) -> "DataFrame":
        if self._attribute.path in self._cache:
            # from cache
            return self._cache[self._attribute.path].values
        if self._attribute.type == AttributeType.FLOAT_SERIES:
            series = FloatSeries()
        elif self._attribute.type == AttributeType.STRING_SERIES:
            series = StringSeries()
        else:
            raise NeptuneUnsupportedType()
        series.values = series.fetch_values(
            self._backend, self._container_id, ContainerType.RUN, [self._attribute.path], include_timestamp
        )
        self._cache[self._attribute.path] = series
        return series.values

    def fetch_last(self) -> Union[str, float]:
        if self._attribute.type == AttributeType.FLOAT_SERIES:
            series = FloatSeries()
        elif self._attribute.type == AttributeType.STRING_SERIES:
            series = StringSeries()
        else:
            raise NeptuneUnsupportedType()
        series.last = series.fetch_last(self._backend, self._container_id, ContainerType.RUN, self._attribute.path)
        self._cache[self._attribute.path] = series
        return series.last


class FetchableSet(Fetchable):
    def fetch(self) -> typing.Set[str]:
        if self._attribute.path in self._cache:
            # from cache
            return self._cache[self._attribute.path].values
        if self._attribute.type == AttributeType.STRING_SET:
            s = StringSet()
        else:
            raise NeptuneUnsupportedType()

        s.values = s.fetch(self._backend, self._container_id, ContainerType.RUN, self._attribute.path)
        self._cache[self._attribute.path] = s
        return s.values


class Downloadable(Fetchable, ABC):
    def fetch(self):
        raise NeptuneUnsupportedType("Files and FileSets do not support `fetch` operation")

    @abstractmethod
    def download(self, destination: Optional[str] = None) -> None:
        ...


class DownloadableFile(Downloadable):
    def download(self, destination: Optional[str] = None) -> None:
        verify_type("destination", destination, (str, type(None)))
        self._backend.download_file(self._container_id, ContainerType.RUN, [self._attribute.path], destination)

    def fetch_extension(self) -> str:
        val = self._backend.get_file_attribute(self._container_id, ContainerType.RUN, [self._attribute.path])
        return val.ext


class DownloadableSet(Downloadable):
    def download(self, destination: Optional[str] = None) -> None:
        verify_type("destination", destination, (str, type(None)))
        self._backend.download_file_set(self._container_id, ContainerType.RUN, [self._attribute.path], destination)


def which_fetchable(attribute: Attribute, *args: Any, **kwargs: Any) -> Fetchable:
    if attribute.type in ATOMS:
        return FetchableAtom(attribute, *args, **kwargs)
    elif attribute.type in SERIES:
        return FetchableSeries(attribute, *args, **kwargs)
    elif attribute.type == AttributeType.STRING_SET:
        return FetchableSet(attribute, *args, **kwargs)
    elif attribute.type == AttributeType.FILE:
        return DownloadableFile(attribute, *args, **kwargs)
    elif attribute.type == AttributeType.FILE_SET:
        return DownloadableSet(attribute, *args, **kwargs)
    raise NeptuneUnsupportedType(f"Unsupported type: {attribute.type}")
