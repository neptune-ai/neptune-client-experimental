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
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    List,
    Optional,
    TypeVar,
    Union,
)

from icecream import ic
from neptune import Project
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
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

from neptune_fetcher.fetchable import (
    Fetchable,
    which_fetchable,
)

T = TypeVar("T")

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


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
        self._backend: NeptuneBackend = HostedNeptuneBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )

        # TODO: Add workspace
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

    def fetch_runs(self, with_ids: List[str]) -> Generator["FrozenProject.FrozenRun", None, None]:
        for run_id in with_ids:
            yield FrozenProject.FrozenRun(
                project=self, container_id=QualifiedName(f"{self.project_identifier}/{run_id}")
            )

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

        def __getitem__(self, item) -> Union[Fetchable]:
            return self._structure[item]

            # self._structure['a/b/c'] = Attribute(type=AttributeType.INT)
            # # fetch
            # self._cache['a/b/c'] = Integer(type=AttributeType.INT, value=4)
            # # clear cache
            # del self._cache['a/b/c']

        def __delitem__(self, key: str) -> None:
            del self._cache[key]

            # queryAttributeDefinitions


if __name__ == "__main__":
    project = FrozenProject(workspace="aleksander.wojnarowicz", project="misc")
    ids = list(map(lambda row: row["sys/id"], project.list_runs()))

    run = next(project.fetch_runs(["MIS-1419"]))

    ic(run["sys/id"].fetch())
    ic(run["sys/owner"].fetch())
    ic(run["sys/owner"].fetch())
    ic(run._cache)
    del run["sys/owner"]
    ic(run._cache)
    ic(run["sys/owner"].fetch())
    ic(run["sys/failed"].fetch())
    ic(run["sys/creation_time"].fetch())
    ic(run["sys/monitoring_time"].fetch())
    ic(run._cache)
    ic(run["monitoring/9401b02f/cpu"].fetch_values())
    ic(run["monitoring/9401b02f/cpu"].fetch_values())
    ic(run["monitoring/9401b02f/cpu"].fetch_last())

    ic(run["source_code/files"].download())
