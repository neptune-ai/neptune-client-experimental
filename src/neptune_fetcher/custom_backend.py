from typing import (
    Any,
    Dict,
    List,
)

from bravado.exception import HTTPNotFound
from neptune.exceptions import ContainerUUIDNotFound
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
)
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.hosted_neptune_backend import (
    HostedNeptuneBackend,
    _logger,
)
from neptune.internal.container_type import ContainerType

from neptune_fetcher.attributes import (
    Attr,
    String,
)


def to_attribute(attr) -> Attribute:
    return Attribute(attr.name, AttributeType(attr.type))


def get_attribute_from_dto(dto: Any) -> Attr:
    if dto.stringProperties is not None:
        return String(AttributeType(dto.type), dto.stringProperties.value)


class CustomBackend(HostedNeptuneBackend):
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
