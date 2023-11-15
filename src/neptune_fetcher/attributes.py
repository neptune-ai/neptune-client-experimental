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

import typing
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from neptune.attributes.atoms.boolean import Boolean as BooleanAttr
from neptune.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.attributes.atoms.float import Float as FloatAttr
from neptune.attributes.atoms.integer import Integer as IntegerAttr
from neptune.attributes.atoms.string import String as StringAttr
from neptune.attributes.series.fetchable_series import Row
from neptune.internal.backends.api_model import (
    AttributeType,
    FloatSeriesValues,
)

T = TypeVar("T")


@dataclass
class Set(Generic[T], ABC):
    values: Optional = None

    @staticmethod
    def fetch(backend, container_id, container_type, path) -> typing.Set[T]:
        ...


class StringSet(Set[str]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> typing.Set[str]:
        return backend.get_string_set_attribute(container_id, container_type, path).values


@dataclass
class Series(ABC):
    values: Optional = None
    last: Optional = None

    def fetch_values(self, backend, container_id, container_type, path, include_timestamp=True):
        import pandas as pd

        limit = 1000
        val = self._fetch_values_from_backend(backend, container_id, container_type, path, 0, limit)
        data = val.values
        offset = limit

        def make_row(entry: Row) -> Dict[str, Union[str, float, datetime]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            row["step"] = entry.step
            row["value"] = entry.value
            if include_timestamp:
                row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)
            return row

        while offset < val.totalItemCount:
            batch = self._fetch_values_from_backend(backend, container_id, container_type, path, offset, limit)
            data.extend(batch.values)
            offset += limit

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient="index")
        return df

    @staticmethod
    def _fetch_values_from_backend(backend, container_id, container_type, path, offset, limit) -> Row:
        ...

    @staticmethod
    def fetch_last(backend, container_id, container_type, path):
        ...


class FloatSeries(Series):
    @staticmethod
    def _fetch_values_from_backend(backend, container_id, container_type, path, offset, limit) -> FloatSeriesValues:
        return backend.get_float_series_values(container_id, container_type, path, offset, limit)

    @staticmethod
    def fetch_last(backend, container_id, container_type, path):
        return backend.get_float_series_attribute(container_id, container_type, [path]).last


class StringSeries(Series):
    @staticmethod
    def _fetch_values_from_backend(backend, container_id, container_type, path, offset, limit) -> Row:
        return backend.get_string_series_values(container_id, container_type, path, offset, limit)

    @staticmethod
    def fetch_last(backend, container_id, container_type, path) -> str:
        return backend.get_string_series_attribute(container_id, container_type, path).last


@dataclass
class Attr(Generic[T], ABC):
    type: AttributeType
    val: Optional[T] = None

    @staticmethod
    def fetch(backend, container_id, container_type, path) -> T:
        ...


class Integer(Attr[int]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> int:
        return IntegerAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class Float(Attr[float]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> float:
        return FloatAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class String(Attr[str]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> str:
        return StringAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class Boolean(Attr[bool]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> bool:
        return BooleanAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class Datetime(Attr[datetime]):
    @staticmethod
    def fetch(backend, container_id, container_type, path) -> datetime:
        return DatetimeAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )
