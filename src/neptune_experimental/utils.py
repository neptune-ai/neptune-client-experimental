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
__all__ = ["wrap_method", "override_method", "override_attr"]

from functools import wraps
from typing import (
    Any,
    Callable,
)


def wrap_method(*, obj: Any, method: str, wrapper: Callable[..., Any]) -> None:
    def wrapped_method_factory(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def new_method(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, original=func, **kwargs)

        return new_method

    override_method(obj=obj, method=method, method_factory=wrapped_method_factory)


def override_method(
    *, obj: Any, method: str, method_factory: Callable[[Callable[..., Any]], Callable[..., Any]]
) -> None:
    source = getattr(obj, method)
    new_method = method_factory(source)

    override_attr(obj=obj, attr=method, new_attr=new_method)


def override_attr(*, obj: Any, attr: str, new_attr: Any) -> None:
    setattr(obj, attr, new_attr)
