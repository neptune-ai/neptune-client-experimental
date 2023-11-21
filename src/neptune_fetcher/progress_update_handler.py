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


class ProgressUpdateHandler(ABC):
    """
    Abstract base class for progress update handlers.

    Example usage:
    >>> from neptune_fetcher import FrozenProject
    >>> from neptune_fetcher.progress_update_handler import ProgressUpdateHandler

    >>> class CustomProgressUpdateHandler(ProgressUpdateHandler):
    >>>     ...  # overwrite the methods you need

    >>> project = FrozenProject(...)
    >>> project.progress_indicator(CustomProgressUpdateHandler())  # use your custom handler to track progress
    """

    def pre_series_fetch(self, total_series: int, series_limit: int) -> None:
        """Runs before a series is fetched. Use it to set the tracking up.

        Parameters:
            :param total_series: Total number of items in the series.
            :param series_limit: Limit of items fetched in a single iteration."""
        ...

    def pre_runs_table_fetch(self) -> None:
        """Runs before a run table is fetched. Use it to set the tracking up."""
        ...

    def on_series_fetch(self, step: int) -> None:
        """Runs after every iteration of a series fetch. Use it to update the progress.

        Parameters:
            :param step: number of items that were fetched during the iteration."""
        ...

    def on_runs_table_fetch(self, step: int) -> None:
        """Runs after every iteration of a run table fetch. Use it to update the progress.

        Parameters:
            :param step: number of items that were fetched during the iteration."""
        ...

    def post_series_fetch(self) -> None:
        """Runs after the series fetch is completed. Use it to clean the tracking up."""
        ...

    def post_runs_table_fetch(self) -> None:
        """Runs after the run table fetch is completed. Use it to clean the tracking up."""
        ...


class NullProgressUpdateHandler(ProgressUpdateHandler):
    ...


class DefaultProgressUpdateHandler(ProgressUpdateHandler):
    def pre_series_fetch(self, total_series: int, series_limit: int) -> None:
        from tqdm import tqdm

        self._series_bar = tqdm(total=total_series)
        self._series_bar.update(n=series_limit)

    def pre_runs_table_fetch(self) -> None:
        from tqdm import tqdm

        self._table_bar = tqdm()

    def on_series_fetch(self, step: int) -> None:
        self._series_bar.update(n=step)
        self._series_bar.set_description("Fetching series values")

    def post_series_fetch(self) -> None:
        self._series_bar.close()

    def on_runs_table_fetch(self, step: int) -> None:
        self._table_bar.update(n=step)
        self._table_bar.set_description("Fetching runs")

    def post_runs_table_fetch(self) -> None:
        self._table_bar.close()
