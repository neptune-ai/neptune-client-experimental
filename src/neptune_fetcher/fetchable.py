from typing import (
    TYPE_CHECKING,
    Dict,
    Union,
)

from neptune.common.warnings import NeptuneUnsupportedType
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
)
from neptune.internal.container_type import ContainerType

from neptune_fetcher.attributes import (
    Attr,
    Boolean,
    Datetime,
    Float,
    FloatSeries,
    Integer,
    Series,
    String,
)

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


ATOMS = {
    AttributeType.INT,
    AttributeType.FLOAT,
    AttributeType.BOOL,
    AttributeType.DATETIME,
    AttributeType.STRING,
    AttributeType.FILE,
    AttributeType.GIT_REF,
    AttributeType.RUN_STATE,
    AttributeType.ARTIFACT,
}
SERIES = {AttributeType.FLOAT_SERIES, AttributeType.STRING_SERIES, AttributeType.FILE_SET, AttributeType.STRING_SET}


class Fetchable:
    def __init__(
        self, attribute: Attribute, backend: "NeptuneBackend", container_id: str, cache: Dict[str, Attr]
    ) -> None:
        self._attribute = attribute
        self._backend = backend
        self._container_id = container_id
        self._cache = cache

    def fetch(self):
        if self._attribute.path in self._cache:
            print("From cache")
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


class FetchableSeries:
    def __init__(
        self, attribute: Attribute, backend: "NeptuneBackend", container_id: str, cache: Dict[str, Union[Attr, Series]]
    ) -> None:
        self._attribute = attribute
        self._backend = backend
        self._container_id = container_id
        self._cache = cache

    def fetch(self):
        raise NeptuneUnsupportedType()

    def fetch_values(self, *, include_timestamp: bool = True):
        if self._attribute.path in self._cache:
            print("from cache")
            return self._cache[self._attribute.path].values
        if self._attribute.type == AttributeType.FLOAT_SERIES:
            series = FloatSeries()
        else:
            raise NeptuneUnsupportedType()
        series.values = series.fetch_values(
            self._backend, self._container_id, ContainerType.RUN, [self._attribute.path], include_timestamp
        )
        self._cache[self._attribute.path] = series
        return series.values

    def fetch_last(self):

        if self._attribute.type == AttributeType.FLOAT_SERIES:
            series = FloatSeries()
        else:
            raise NeptuneUnsupportedType()
        series.last = series.fetch_last(self._backend, self._container_id, ContainerType.RUN, self._attribute.path)
        self._cache[self._attribute.path] = series
        return series.last


def fetchable_or_fetchable_series(attribute: Attribute, *args, **kwargs) -> Union[Fetchable, FetchableSeries]:
    if attribute.type in ATOMS:
        return Fetchable(attribute, *args, **kwargs)
    elif attribute.type in SERIES:
        return FetchableSeries(attribute, *args, **kwargs)
    raise NeptuneUnsupportedType()
