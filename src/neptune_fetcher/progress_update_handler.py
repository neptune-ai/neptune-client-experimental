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
    """Abstract base class for progress update handlers.

    Example usage:
    >>> from neptune_fetcher import ReadOnlyProject
    >>> from neptune_fetcher.progress_update_handler import ProgressUpdateHandler

    >>> class CustomProgressUpdateHandler(ProgressUpdateHandler):
    >>>     ...  # overwrite the methods you need

    >>> project = ReadOnlyProject(...)
    >>> project.progress_indicator(CustomProgressUpdateHandler())  # use your custom handler to track progress
    """

    def pre_series_fetch(self, total_series: int, series_limit: int) -> None:
        """Executes before a series is fetched. Use it to set up the tracking up.

        Args:
            total_series: Total number of items in the series.
            series_limit: Limit of items fetched in a single iteration.
        """
        ...

    def pre_runs_table_fetch(self) -> None:
        """Executes before a run table is fetched. Use it to set the tracking up."""
        ...

    def pre_download(self, total_size: int) -> None:
        """Executes before a file or a fileset is downloaded. Use it to set the tracking up.

        Args:
            total_size: Total size of the file.
        """
        ...

    def on_series_fetch(self, step: int) -> None:
        """Executes after every iteration of a series fetch. Use it to update the progress.

        Args:
            step: number of items that were fetched during the iteration."""
        ...

    def on_runs_table_fetch(self, step: int) -> None:
        """Executes after every iteration of a runs table fetch. Use it to update the progress.

        Args:
            step: number of items that were fetched during the iteration."""
        ...

    def post_series_fetch(self) -> None:
        """Executes after the series fetch is completed. Use it to clean the tracking up."""
        ...

    def post_runs_table_fetch(self) -> None:
        """Executes after the run table fetch is completed. Use it to clean the tracking up."""

    def on_download_chunk(self, chunk: int) -> None:
        """Executes after every iteration of a file or fileset download. Use it to update the progress.

        Args:
            chunk: Number of bytes that were fetched during the iteration."""
        ...

    def post_download(self) -> None:
        """Executes after the download is completed. Use it to clean the tracking up."""
        ...


class NullProgressUpdateHandler(ProgressUpdateHandler):
    ...


class DefaultProgressUpdateHandler(ProgressUpdateHandler):
    def pre_series_fetch(self, total_series: int, series_limit: int) -> None:
        from tqdm import tqdm

        self._series_bar = tqdm(total=total_series, desc="Fetching series values", unit=" steps")
        self._series_bar.update(n=series_limit)

    def pre_runs_table_fetch(self) -> None:
        from tqdm import tqdm

        self._table_bar = tqdm(desc="Fetching runs", unit=" runs")

    def on_series_fetch(self, step: int) -> None:
        self._series_bar.update(n=step)

    def post_series_fetch(self) -> None:
        self._series_bar.close()

    def on_runs_table_fetch(self, step: int) -> None:
        self._table_bar.update(n=step)

    def post_runs_table_fetch(self) -> None:
        self._table_bar.close()

    def pre_download(self, total_size: int) -> None:
        from tqdm import tqdm

        self._download_bar = tqdm(total=total_size, desc="Downloading file", unit="B", unit_scale=True)

    def on_download_chunk(self, chunk: int) -> None:
        self._download_bar.update(n=chunk)

    def post_download(self) -> None:
        self._download_bar.close()
