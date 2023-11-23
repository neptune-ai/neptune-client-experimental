from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Optional,
    Union,
)

from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.metadata_containers.metadata_containers_table import TableEntry

from neptune_fetcher.fetchable import (
    Downloadable,
    Fetchable,
    FetchableSeries,
    which_fetchable,
)

if TYPE_CHECKING:
    from neptune_fetcher.read_only_project import ReadOnlyProject


def _get_attribute(entry: TableEntry, name: str) -> Optional[str]:
    try:
        return entry.get_attribute_value(name)
    except ValueError:
        return None


class ReadOnlyRun:
    def __init__(self, project: "ReadOnlyProject", with_id: Optional[str] = None):
        self.project = project
        self.with_id = with_id
        self._container_id = QualifiedName(f"{self.project.project_identifier}/{with_id}")
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
