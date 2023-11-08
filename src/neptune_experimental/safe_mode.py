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

import functools
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Tuple,
    Type,
)

from neptune_experimental.env import NEPTUNE_SAFETY_MODE
from neptune_experimental.utils import override_method

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer


def initialize() -> None:
    override_run()
    override_project()
    override_model_version()
    override_model()
    override_handler()


def override_common_operations(obj: Type["MetadataContainer"]) -> None:
    from neptune.handler import Handler

    override_method(obj=obj, method="__getitem__", method_factory=safe_function(Handler(None, None)))
    override_method(obj=obj, method="__setitem__", method_factory=safe_function())
    override_method(obj=obj, method="__delitem__", method_factory=safe_function())
    override_method(obj=obj, method="assign", method_factory=safe_function())
    override_method(obj=obj, method="fetch", method_factory=safe_function({}))
    override_method(obj=obj, method="ping", method_factory=safe_function())
    override_method(obj=obj, method="start", method_factory=safe_function())
    override_method(obj=obj, method="stop", method_factory=safe_function())
    override_method(obj=obj, method="get_state", method_factory=safe_function())
    override_method(obj=obj, method="get_structure", method_factory=safe_function({}))
    override_method(obj=obj, method="print_structure", method_factory=safe_function())
    override_method(obj=obj, method="define", method_factory=safe_function())
    override_method(obj=obj, method="get_attribute", method_factory=safe_function())
    override_method(obj=obj, method="set_attribute", method_factory=safe_function())
    override_method(obj=obj, method="exists", method_factory=safe_function(False))
    override_method(obj=obj, method="pop", method_factory=safe_function())
    override_method(obj=obj, method="wait", method_factory=safe_function())
    override_method(obj=obj, method="sync", method_factory=safe_function())
    override_method(obj=obj, method="get_root_object", method_factory=safe_function())
    override_method(obj=obj, method="get_url", method_factory=safe_function())


def override_model() -> None:
    from neptune import Model

    override_common_operations(obj=Model)
    override_method(obj=Model, method="fetch_model_versions_table", method_factory=safe_function())


def override_model_version() -> None:
    from neptune import ModelVersion

    override_common_operations(obj=ModelVersion)
    override_method(obj=ModelVersion, method="change_stage", method_factory=safe_function())


def override_project() -> None:
    from neptune import Project

    override_common_operations(obj=Project)
    override_method(obj=Project, method="fetch_runs_table", method_factory=safe_function())
    override_method(obj=Project, method="fetch_models_table", method_factory=safe_function())


def override_run() -> None:
    from neptune import Run

    override_common_operations(obj=Run)
    override_method(obj=Run, method="monitoring_namespace", method_factory=safe_function())


def override_handler() -> None:
    from neptune.handler import Handler

    override_method(obj=Handler, method="__getitem__", method_factory=safe_function())
    override_method(obj=Handler, method="__setitem__", method_factory=safe_function())
    override_method(obj=Handler, method="__getattr__", method_factory=safe_function())
    override_method(obj=Handler, method="get_root_object", method_factory=safe_function())
    override_method(obj=Handler, method="assign", method_factory=safe_function())
    override_method(obj=Handler, method="upload", method_factory=safe_function())
    override_method(obj=Handler, method="upload_files", method_factory=safe_function())
    override_method(obj=Handler, method="log", method_factory=safe_function())
    override_method(obj=Handler, method="append", method_factory=safe_function())
    override_method(obj=Handler, method="extend", method_factory=safe_function())
    override_method(obj=Handler, method="add", method_factory=safe_function())
    override_method(obj=Handler, method="pop", method_factory=safe_function())
    override_method(obj=Handler, method="remove", method_factory=safe_function())
    override_method(obj=Handler, method="clear", method_factory=safe_function())
    override_method(obj=Handler, method="delete_files", method_factory=safe_function())
    override_method(obj=Handler, method="download", method_factory=safe_function())
    override_method(obj=Handler, method="download_last", method_factory=safe_function())
    override_method(obj=Handler, method="fetch_hash", method_factory=safe_function())
    override_method(obj=Handler, method="fetch_extension", method_factory=safe_function())
    override_method(obj=Handler, method="fetch_files_list", method_factory=safe_function([]))
    override_method(obj=Handler, method="list_fileset_files", method_factory=safe_function([]))
    override_method(obj=Handler, method="track_files", method_factory=safe_function())


def safe_function(default_return_value: Any = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    from neptune.common.warnings import warn_once

    safe_mode_enabled = os.getenv(NEPTUNE_SAFETY_MODE, "false").lower() in ("true", "1", "t")

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Tuple, **kwargs: Dict[str, Any]) -> Any:
            if safe_mode_enabled:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    try:
                        warn_once(f"Exception in method {func}: {ex.__class__.__name__}")
                    except Exception:
                        pass
                    return default_return_value
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorator
