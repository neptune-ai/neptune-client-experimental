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
__all__ = ["initialize"]

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)

from neptune_experimental.utils import wrap_method

if TYPE_CHECKING:
    from neptune.metadata_containers import Run


def initialize() -> None:
    from neptune import Run

    wrap_method(obj=Run, method="__init__", wrapper=custom_init)


def custom_init(
    self: "Run", *args: Any, original: Callable[..., Any], name: Optional[str] = None, **kwargs: Any
) -> None:
    original(self, *args, name=name, **kwargs)
    if name is None:
        self._name = self._api_object.sys_id
        self["sys/name"] = self._name
