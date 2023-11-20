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
    "ProgressUpdateHandler",
    "NullProgressUpdateHandler",
    "DefaultProgressUpdateHandler",
]

from abc import ABC

from tqdm import tqdm


class ProgressUpdateHandler(ABC):
    def series_setup(self, total_series: int, series_limit: int) -> None:
        ...

    def table_setup(self) -> None:
        ...

    def on_series_fetch(self, step: int) -> None:
        ...

    def post_series_fetch(self) -> None:
        ...

    def on_run_table_fetch(self, step: int) -> None:
        ...

    def post_table_fetch(self) -> None:
        ...


class NullProgressUpdateHandler(ProgressUpdateHandler):
    ...


class DefaultProgressUpdateHandler(ProgressUpdateHandler):
    def series_setup(self, total_series: int, series_limit: int) -> None:
        self._series_bar = tqdm(total=total_series)
        self._series_bar.update(n=series_limit)

    def table_setup(self) -> None:
        self._table_bar = tqdm()

    def on_series_fetch(self, step: int) -> None:
        self._series_bar.update(n=step)
        self._series_bar.set_description("Fetching series values")

    def post_series_fetch(self) -> None:
        self._series_bar.close()

    def on_run_table_fetch(self, step: int) -> None:
        self._table_bar.update(n=step)
        self._table_bar.set_description("Fetching runs dataframe")

    def post_table_fetch(self) -> None:
        self._table_bar.close()
