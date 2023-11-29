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
__all__ = ["CustomBackend"]

import os
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
)

from bravado.exception import HTTPNotFound
from neptune.api.searching_entries import iter_over_pages
from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.exceptions import ClientHttpError
from neptune.envs import NEPTUNE_FETCH_TABLE_STEP_SIZE
from neptune.exceptions import (
    ContainerUUIDNotFound,
    FetchAttributeNotFoundException,
    ProjectNotFound,
)
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
    LeaderboardEntry,
)
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.hosted_file_operations import (
    download_file_attribute,
    download_file_set_attribute,
)
from neptune.internal.backends.hosted_neptune_backend import (
    HostedNeptuneBackend,
    _logger,
)
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.utils.paths import path_to_str

from neptune_fetcher.attributes import (
    Attr,
    Boolean,
    Datetime,
    Float,
    Integer,
    String,
)
from neptune_fetcher.progress_update_handler import (
    NullProgressUpdateHandler,
    ProgressUpdateHandler,
)


def to_attribute(attr) -> Attribute:
    return Attribute(attr.name, AttributeType(attr.type))


def get_attribute_from_dto(dto: Any) -> Attr:
    if dto.stringProperties is not None:
        return String(AttributeType(dto.type), dto.stringProperties.value)
    if dto.boolProperties is not None:
        return Boolean(AttributeType(dto.type), dto.boolProperties.value)
    if dto.floatProperties is not None:
        return Float(AttributeType(dto.type), dto.floatProperties.value)
    if dto.intProperties is not None:
        return Integer(AttributeType(dto.type), dto.intProperties.value)
    if dto.datetimeProperties is not None:
        return Datetime(AttributeType(dto.type), dto.datetimeProperties.value)
    raise Exception(f"Attribute {dto.name} of type {AttributeType(dto.type)} does not support prefetching")


class CustomBackend(HostedNeptuneBackend):
    def __init__(self, *args: Any, progress_update_handler: Optional[ProgressUpdateHandler] = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.progress_update_handler: ProgressUpdateHandler = (
            progress_update_handler if progress_update_handler else NullProgressUpdateHandler()
        )

    def download_file(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
    ) -> None:
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                container_id=container_id,
                attribute=path_to_str(path),
                destination=destination,
                pre_download_hook=self.progress_update_handler.pre_download,
                download_iter_hook=self.progress_update_handler.on_download_chunk,
                post_download_hook=self.progress_update_handler.post_download,
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file_set(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
    ) -> None:
        download_request = self._get_file_set_download_request(container_id, container_type, path)
        try:
            download_file_set_attribute(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination,
                pre_download_hook=self.progress_update_handler.pre_download,
                download_iter_hook=self.progress_update_handler.on_download_chunk,
                post_download_hook=self.progress_update_handler.post_download,
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        params = {
            "experimentIdentifier": container_id,
            **DEFAULT_REQUEST_KWARGS,
        }
        try:
            experiment = self.leaderboard_client.api.queryAttributeDefinitions(**params).response().result

            attribute_type_names = [at.value for at in AttributeType]
            accepted_attributes = [attr for attr in experiment.entries if attr.type in attribute_type_names]

            # Notify about ignored attrs
            ignored_attributes = set(attr.type for attr in experiment.entries) - set(
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

    def prefetch_values(self, container_id: str, container_type: ContainerType, paths: List[str]) -> Dict[str, Attr]:
        params = {
            "holderIdentifier": container_id,
            "holderType": "experiment",
            "attributeQuery": {
                "attributePathsFilter": paths,
            },
            **DEFAULT_REQUEST_KWARGS,
        }

        try:
            result = self.leaderboard_client.api.getAttributesWithPathsFilter(**params).response().result
        except HTTPNotFound as e:
            raise ContainerUUIDNotFound(
                container_id=container_id,
                container_type=container_type,
            ) from e

        return {dto.name: get_attribute_from_dto(dto) for dto in result.attributes}

    @with_api_exceptions_handler
    def search_leaderboard_entries(
        self,
        project_id: UniqueId,
        types: Optional[Iterable[ContainerType]] = None,
        query: Optional[NQLQuery] = None,
        columns: Optional[Iterable[str]] = None,
    ) -> List[LeaderboardEntry]:
        step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "1000"))

        types_filter = list(map(lambda container_type: container_type.to_api(), types)) if types else None
        attributes_filter = {"attributeFilters": [{"path": column} for column in columns]} if columns else {}

        try:
            items = []
            self.progress_update_handler.pre_runs_table_fetch()

            for entry in iter_over_pages(
                client=self.leaderboard_client,
                project_id=project_id,
                types=types_filter,
                query=query,
                attributes_filter=attributes_filter,
                step_size=step_size,
            ):
                items.append(entry)
                self.progress_update_handler.on_runs_table_fetch(1)

            self.progress_update_handler.post_runs_table_fetch()
            return items
        except HTTPNotFound:
            raise ProjectNotFound(project_id)
