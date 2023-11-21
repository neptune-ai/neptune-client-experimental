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
    "ReadOnlyProject",
]

from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
)

from neptune import Project
from neptune.internal.backends.nql import NQLEmptyQuery
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.management.internal.utils import normalize_project_name
from neptune.metadata_containers.metadata_containers_table import (
    Table,
    TableEntry,
)
from neptune.metadata_containers.utils import prepare_nql_query

from neptune_fetcher.custom_backend import CustomBackend
from neptune_fetcher.fetchable import (
    Downloadable,
    Fetchable,
    FetchableSeries,
    which_fetchable,
)
from neptune_fetcher.progress_update_handler import (
    DefaultProgressUpdateHandler,
    NullProgressUpdateHandler,
    ProgressUpdateHandler,
)

if TYPE_CHECKING:
    from pandas import DataFrame

T = TypeVar("T")


def _get_attribute(entry: TableEntry, name: str) -> Optional[str]:
    try:
        return entry.get_attribute_value(name)
    except ValueError:
        return None


class ReadOnlyProject:
    """Class for retrieving metadata from a neptune.ai project in a limited read-only mode."""

    def __init__(
        self,
        project: Optional[str] = None,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None,
        proxies: Optional[dict] = None,
    ) -> None:
        """Initializes a Neptune project in limited read-only mode.

        Compared to a regular Project object, it contains only basic project information and exposes more lightweight
        methods for fetching run metadata.

        Args:
            project: The name of the Neptune project.
            workspace: The workspace associated with the project.
            api_token: Neptune account's API token.
                If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
            proxies: A dictionary of proxy settings if needed.
        """
        self._project: Optional[str] = project
        self._backend: CustomBackend = CustomBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )

        self.project_identifier = normalize_project_name(name=project, workspace=workspace)
        self._project_api_object: Project = project_name_lookup(backend=self._backend, name=self.project_identifier)
        self._project_id: UniqueId = self._project_api_object.id

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
        """Lists IDs and names of the runs in the project.

        Returns a generator of run info dictionaries `{"sys/id": ..., "sys/name": ...}`.
        """
        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=NQLEmptyQuery(),
            columns=["sys/id", "sys/name"],
        )

        for row in Table(
            backend=self._backend, container_type=ContainerType.RUN, entries=leaderboard_entries
        ).to_rows():
            yield {
                "sys/id": _get_attribute(entry=row, name="sys/id"),
                "sys/name": _get_attribute(entry=row, name="sys/name"),
            }

    def fetch_read_only_runs(self, with_ids: List[str]) -> Generator["ReadOnlyProject.ReadOnlyRun", None, None]:
        """Lists runs of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyProject.ReadOnlyRun` instances.

        Args:
            with_ids: List of run ids to fetch.
        """
        for run_id in with_ids:
            yield ReadOnlyProject.ReadOnlyRun(
                project=self, container_id=QualifiedName(f"{self.project_identifier}/{run_id}")
            )

    def fetch_runs(self) -> "DataFrame":
        """Fetches a table containing IDs and names of runs in the project.

        Returns `pandas.DataFrame` with two columns ('sys/id' and 'sys/name') and rows corresponding to project runs.
        """
        return self.fetch_runs_df(columns=["sys/id", "sys/name"])

    def progress_indicator(self, handler: Union[ProgressUpdateHandler, bool]) -> None:
        """Sets or resets a progress indicator handler to track download progress.

        The progress concerns the downloading of files/filesets, fetching series values, and fetching the runs table
        from a Neptune project.

        Args:
            handler: Either a boolean value or a `ProgressUpdateHandler` instance.
                If `ProgressUpdateHandler` instance - will use this instance to track progress.
                If `True` - equivalent to using `DefaultProgressUpdateHandler`.
                If `False` - resets progress indicator (no progress update will be performed).
        """
        if isinstance(handler, bool):
            if handler:
                self._backend.progress_update_handler = DefaultProgressUpdateHandler()
            else:
                # reset
                self._backend.progress_update_handler = NullProgressUpdateHandler()
        else:
            self._backend.progress_update_handler = handler

    def fetch_runs_df(
        self,
        columns: Optional[Iterable[str]] = None,
        with_ids: Optional[Iterable[str]] = None,
        states: Optional[Iterable[str]] = None,
        owners: Optional[Iterable[str]] = None,
        tags: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
    ) -> "DataFrame":
        """Fetches the runs' metadata and returns them as a pandas DataFrame.

        Args:
            columns: A list of column names to include in the DataFrame.
                Defaults to None, which includes all available columns.
            with_ids: A list of run IDs to filter the results.
            states: A list of run states to filter the results.
            owners: A list of owner names to filter the results.
            tags: A list of tags to filter the results.
            trashed: Whether to return trashed runs as the result.
                If True: return only trashed runs.
                If False (default): return only non-trashed runs.
                If None: return all runs.

        Returns:
            DataFrame: A pandas DataFrame containing information about the fetched runs.

        Example:
            ```
            # Fetch all runs with specific columns
            columns_to_fetch = ["sys/name", "sys/modification_time", "training/lr"]
            runs_df = my_project.fetch_runs_df(columns=columns_to_fetch, states=["active"])

            # Fetch runs by specific IDs
            specific_run_ids = ["RUN-123", "RUN-456"]
            specific_runs_df = my_project.fetch_runs_df(with_ids=specific_run_ids)
            ```
        """
        query = prepare_nql_query(with_ids, states, owners, tags, trashed)

        if columns is not None:
            # always return entries with `sys/id` column when filter applied
            columns = set(columns)
            columns.add("sys/id")

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=query,
            columns=columns,
        )

        return Table(
            backend=self._backend,
            container_type=ContainerType.RUN,
            entries=leaderboard_entries,
        ).to_pandas()

    class ReadOnlyRun:
        def __init__(self, project: "ReadOnlyProject", container_id: QualifiedName) -> None:
            self._container_id = container_id
            self.project = project
            self._cache = dict()
            self._structure = {
                attribute.path: which_fetchable(
                    attribute,
                    self.project._backend,
                    self._container_id,
                    self._cache,
                )
                for attribute in self.project._backend.get_attributes(self._container_id, ContainerType.RUN)
            }

        def __getitem__(self, item: str) -> Union[Fetchable, FetchableSeries, Downloadable]:
            return self._structure[item]

        def __delitem__(self, key: str) -> None:
            del self._cache[key]

        @property
        def field_names(self) -> Generator[str, None, None]:
            """Lists names of run fields.

            Returns a generator of run fields.
            """
            yield from self._structure

        def prefetch(self, paths: List[str]) -> None:
            """Prefetches values of a list of fields and stores them in local cache.

            Args:
                paths: List of field paths to prefetch.
            """
            fetched = self.project._backend.prefetch_values(self._container_id, ContainerType.RUN, paths)
            self._cache.update(fetched)