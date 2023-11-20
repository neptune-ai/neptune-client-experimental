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
        self._project: Optional[str] = project
        self._backend: CustomBackend = CustomBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )

        self.project_identifier = normalize_project_name(name=project, workspace=workspace)
        self._project_api_object: Project = project_name_lookup(backend=self._backend, name=self.project_identifier)
        self._project_id: UniqueId = self._project_api_object.id

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
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
        for run_id in with_ids:
            yield FrozenProject.FrozenRun(
                project=self, container_id=QualifiedName(f"{self.project_identifier}/{run_id}")
            )

    def fetch_runs(self) -> "DataFrame":
        return self.fetch_runs_df(columns=["sys/id", "sys/name"])

    def progress_indicator(self, handler: Union[ProgressUpdateHandler, bool]) -> None:
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
            yield from self._structure

        def prefetch(self, paths: List[str]) -> None:
            fetched = self.project._backend.prefetch_values(self._container_id, ContainerType.RUN, paths)
            self._cache.update(fetched)
