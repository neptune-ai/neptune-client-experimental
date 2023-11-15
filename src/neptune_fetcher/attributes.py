from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Generic,
    Optional,
    TypeVar,
)

from neptune.attributes.atoms.boolean import Boolean as BooleanAttr
from neptune.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.attributes.atoms.float import Float as FloatAttr
from neptune.attributes.atoms.integer import Integer as IntegerAttr
from neptune.attributes.atoms.string import String as StringAttr
from neptune.internal.backends.api_model import AttributeType

T = TypeVar("T")


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
