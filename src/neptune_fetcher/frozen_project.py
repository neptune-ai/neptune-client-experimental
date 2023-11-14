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
from abc import ABC
from dataclasses import dataclass
from typing import (
    Optional,
    TYPE_CHECKING,
    Generator, List, Dict, TypeVar, Generic,
)

from neptune import Project
from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.envs import API_TOKEN_ENV_NAME
from neptune.internal.backends.api_model import Attribute, AttributeType
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.credentials import Credentials

from neptune.internal.backends.factory import get_backend
from neptune.internal.id_formats import QualifiedName, UniqueId
from neptune.management.internal.utils import normalize_project_name
from neptune.types.mode import Mode
from neptune.internal.container_type import ContainerType
from neptune.internal.backends.nql import NQLEmptyQuery
from neptune.metadata_containers.metadata_containers_table import Table, TableEntry

from icecream import ic


if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


def _get_attribute(entry: TableEntry, name: str) -> Optional[str]:
    try:
        return entry.get_attribute_value(name)
    except ValueError:
        return None


class CustomBackend(HostedNeptuneBackend):
    @with_api_exceptions_handler
    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        def to_attribute(attr) -> Attribute:
            return Attribute(attr.name, AttributeType(attr.type))

        params = {
            "experimentId": container_id,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            experiment = self.leaderboard_client.api.queryAttributeDefinitions(**params).response().result

            attribute_type_names = [at.value for at in AttributeType]
            accepted_attributes = [attr for attr in experiment.attributes if attr.type in attribute_type_names]

            # Notify about ignored attrs
            ignored_attributes = set(attr.type for attr in experiment.attributes) - set(
                attr.type for attr in accepted_attributes
            )
            if ignored_attributes:
                _logger.warning(
                    "Ignored following attributes (unknown type): %s.\n" "Try to upgrade `neptune`.",
                    ignored_attributes,
                )

            return [to_attribute(attr) for attr in accepted_attributes if attr.type in attribute_type_names]
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e


T = TypeVar("T")


@dataclass
class Attribute(Generic[T], ABC):
    type: AttributeType
    value: T

    @staticmethod
    def fetch(backend, container_id, container_type, path) -> T:
        ...


from neptune.attributes.atoms.integer import Integer as IntegerAttr
from neptune.attributes.atoms.string import String as StringAttr

@dataclass
class Integer(Attribute[int]):

    @staticmethod
    def fetch(backend, container_id, container_type, path) -> int:
        return IntegerAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


@dataclass
class String(Attribute[str]):

    @staticmethod
    def fetch(backend, container_id, container_type, path) -> str:
        return StringAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class FrozenProject:
    def __init__(
            self,
            project: Optional[str] = None,
            api_token: Optional[str] = None,
            proxies: Optional[dict] = None
    ) -> None:
        self._project: Optional[str] = project
        self._backend: NeptuneBackend = HostedNeptuneBackend(
            credentials=Credentials.from_token(api_token=api_token),
            proxies=proxies
        )

        # TODO: Add workspace
        self.project_identifier = normalize_project_name(name=project, workspace=None)
        self._project_api_object: Project = project_name_lookup(
            backend=self._backend, name=self.project_identifier
        )
        self._project_id: UniqueId = self._project_api_object.id

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=NQLEmptyQuery(),
            columns=["sys/id", "sys/name"],
        )

        for row in Table(
                backend=self._backend,
                container_type=ContainerType.RUN,
                entries=leaderboard_entries
        ).to_rows():
            yield {
                "sys/id": _get_attribute(entry=row, name='sys/id'),
                "sys/name": _get_attribute(entry=row, name='sys/name'),
            }

    def fetch_runs(self, with_ids: List[str]) -> Generator["FrozenProject.FrozenRun", None, None]:
        for run_id in with_ids:
            yield FrozenProject.FrozenRun(
                project=self,
                container_id=QualifiedName(f"{self.project_identifier}/{run_id}")
            )

    class FrozenRun:
        def __init__(self, project: "FrozenProject", container_id: QualifiedName) -> None:
            self._container_id = container_id
            self.project = project
            self._structure = dict()
            self._cache = dict()

            self._structure['a/b/c'] = Attribute(type=AttributeType.INT)
            # fetch
            self._cache['a/b/c'] = Integer(type=AttributeType.INT, value=4)
            # clear cache
            del self._cache['a/b/c']

        def __delattr__(self, item) -> None:
            del self._cache[item]

            # queryAttributeDefinitions


if __name__ == '__main__':
    project = FrozenProject(project="rafal.neptune/test")
    ids = list(map(lambda row: row['sys/id'], project.list_runs()))

    for run_info in project.fetch_runs(with_ids=ids):
        ic(run_info._container_id)
