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
    "FrozenProject",
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


class FrozenProject:
    def __init__(
        self,
        project: Optional[str] = None,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None,
        proxies: Optional[dict] = None,
    ) -> None:
        """
        Initializes a new FrozenProject instance.

        Parameters:
        - project (Optional[str]): The name of the project. Defaults to None.
        - workspace (Optional[str]): The workspace associated with the project. Defaults to None.
        - api_token (Optional[str]): User's API token. Defaults to None.
                If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        - proxies (Optional[dict]): A dictionary of proxy settings if needed. Defaults to None."""
        self._project: Optional[str] = project
        self._backend: CustomBackend = CustomBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )

        self.project_identifier = normalize_project_name(name=project, workspace=workspace)
        self._project_api_object: Project = project_name_lookup(backend=self._backend, name=self.project_identifier)
        self._project_id: UniqueId = self._project_api_object.id

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
        """
        List ids and names of the runs in the project.
        :return: Generator of run info dictionaries `{"sys/id": ..., "sys/name": ...}`"""
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

    def fetch_frozen_runs(self, with_ids: List[str]) -> Generator["FrozenProject.FrozenRun", None, None]:
        """
        List project run object in the form of `FrozenRun` generator (read-only runs).

        :param with_ids: List of run ids to fetch
        :return: Generator of `FrozenProject.FrozenRun` instances.
        """
        for run_id in with_ids:
            yield FrozenProject.FrozenRun(
                project=self, container_id=QualifiedName(f"{self.project_identifier}/{run_id}")
            )

    def fetch_runs(self) -> "DataFrame":
        """
        Fetch a table containing ids and names of project runs.
        :return: `pandas.DataFrame` with two columns ('sys/id' and 'sys/name') and rows corresponding to project runs.
        """
        return self.fetch_runs_df(columns=["sys/id", "sys/name"])

    def progress_indicator(self, handler: Union[ProgressUpdateHandler, bool]) -> None:
        """
        Set or reset progress indicator handler to track progress of downloading files/file sets, fetching series values
        or run tables.

        :param handler: Either a boolean value or a `ProgressUpdateHandler` instance.
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
        """
        Fetches runs information and returns it as a pandas DataFrame.

        Parameters:
        - columns (Optional[Iterable[str]]): A list of column names to include in the DataFrame.
          Defaults to None, which includes all available columns.
        - with_ids (Optional[Iterable[str]]): A list of run IDs to filter the results. Defaults to None.
        - states (Optional[Iterable[str]]): A list of run states to filter the results. Defaults to None.
        - owners (Optional[Iterable[str]]): A list of owner names to filter the results. Defaults to None.
        - tags (Optional[Iterable[str]]): A list of tags to filter the results. Defaults to None.
        - trashed (Optional[bool]): Whether to return trashed runs as the result. Defaults to False.
            If True: return only trashed runs.
            If False: return only non-trashed runs.
            If None: return all runs.

        Returns:
        - DataFrame: A pandas DataFrame containing information about the fetched runs.

        Example:
        ```
        # Fetch all runs with specific columns
        columns_to_fetch = ["sys/name", "sys/modification_time", "training/lr"]
        runs_df = my_project.fetch_runs_df(columns=columns_to_fetch, states=["active"])

        # Fetch runs by specific IDs
        specific_run_ids = ["run123", "run456"]
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

    class FrozenRun:
        def __init__(self, project: "FrozenProject", container_id: QualifiedName) -> None:
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
            """
            List names of run fields.
            :return: Generator of run fields.
            """
            yield from self._structure

        def prefetch(self, paths: List[str]) -> None:
            """
            Prefetch values of a list of fields and store them in local cache.
            :param paths: List of field paths to prefetch
            """
            fetched = self.project._backend.prefetch_values(self._container_id, ContainerType.RUN, paths)
            self._cache.update(fetched)
