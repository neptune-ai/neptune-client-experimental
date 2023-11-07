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
__all__ = ["NeptuneFetcher"]

from datetime import datetime
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
)

from neptune.attributes import File  # noqa: F401
from neptune.attributes import FileSeries  # noqa: F401
from neptune.attributes import FileSet  # noqa: F401
from neptune.attributes import FloatSeries  # noqa: F401
from neptune.attributes import StringSeries  # noqa: F401
from neptune.attributes import StringSet  # noqa: F401
from neptune.attributes import (
    Boolean,
    Datetime,
    Float,
    Integer,
    String,
)
from neptune.internal.backends.factory import get_backend
from neptune.internal.id_formats import QualifiedName
from neptune.types.mode import Mode

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


class NeptuneFetcher:
    def __init__(
        self, project: str, with_id: str, api_token: Optional[str] = None, proxies: Optional[dict] = None
    ) -> None:
        self._backend: NeptuneBackend = get_backend(mode=Mode.READ_ONLY, api_token=api_token, proxies=proxies)
        self._container_id = self._backend.get_metadata_container(
            container_id=QualifiedName(f"{project}/{with_id}"),
            expected_container_type=None,
        ).id

    def close(self) -> None:
        self._backend.close()

    def _call_getter(self, attribute_cls, path):
        return attribute_cls.getter(
            backend=self._backend,
            container_id=self._container_id,
            container_type=None,
            path=path,
        )

    def fetch_boolean(self, path: List[str]) -> bool:
        return self._call_getter(Boolean, path=path)

    def fetch_datetime(self, path: List[str]) -> datetime:
        return self._call_getter(Datetime, path=path)

    def fetch_float(self, path: List[str]) -> float:
        return self._call_getter(Float, path=path)

    def fetch_integer(self, path: List[str]) -> int:
        return self._call_getter(Integer, path=path)

    def fetch_string(self, path: List[str]) -> str:
        return self._call_getter(String, path=path)

    def __enter__(self) -> "NeptuneFetcher":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


def main():
    with NeptuneFetcher(project="rafal.neptune/test", with_id="TES-157") as fetcher:
        print(fetcher.fetch_string(["foo", "bar"]))
        print(fetcher.fetch_boolean(["foo", "doe"]))


if __name__ == "__main__":
    main()
