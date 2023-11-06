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
    Any,
    Dict,
    Tuple, Callable,
)

from neptune import Run, Project, ModelVersion, Model
from neptune.common.warnings import warn_once
from neptune.handler import Handler

from neptune_experimental.utils import override_method
from neptune_experimental.experimental_envs import NEPTUNE_SAFETY_MODE


def initialize() -> None:
    override_run()
    override_project()
    override_model_version()
    override_model()
    override_handler()


def override_model() -> None:
    override_method(
        obj=Model,
        method="__getitem__",
        method_factory=safe_function(Handler(None, None)))
    override_method(obj=Model, method="__setitem__", method_factory=safe_function(None))
    override_method(obj=Model, method="assign", method_factory=safe_function(None))
    override_method(obj=Model, method="fetch", method_factory=safe_function({}))
    override_method(obj=Model, method="ping", method_factory=safe_function(None))
    override_method(obj=Model, method="start", method_factory=safe_function(None))
    override_method(obj=Model, method="stop", method_factory=safe_function(None))
    override_method(obj=Model, method="get_state", method_factory=safe_function(None))
    override_method(obj=Model, method="get_structure", method_factory=safe_function({}))
    override_method(obj=Model, method="print_structure", method_factory=safe_function(None))
    override_method(obj=Model, method="define", method_factory=safe_function(None))
    override_method(obj=Model, method="get_attribute", method_factory=safe_function(None))
    override_method(obj=Model, method="set_attribute", method_factory=safe_function(None))
    override_method(obj=Model, method="exists", method_factory=safe_function(False))
    override_method(obj=Model, method="pop", method_factory=safe_function(None))
    override_method(obj=Model, method="wait", method_factory=safe_function(None))
    override_method(obj=Model, method="sync", method_factory=safe_function(None))
    override_method(obj=Model, method="get_root_object", method_factory=safe_function(None))
    override_method(obj=Model, method="get_url", method_factory=safe_function(None))
    override_method(obj=Model, method="fetch_model_versions_table", method_factory=safe_function(None))


def override_model_version() -> None:
    override_method(
        obj=ModelVersion,
        method="__getitem__",
        method_factory=safe_function(Handler(None, None)))
    override_method(obj=ModelVersion, method="__setitem__", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="assign", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="fetch", method_factory=safe_function({}))
    override_method(obj=ModelVersion, method="ping", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="start", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="stop", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="get_state", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="get_structure", method_factory=safe_function({}))
    override_method(obj=ModelVersion, method="print_structure", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="define", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="get_attribute", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="set_attribute", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="exists", method_factory=safe_function(False))
    override_method(obj=ModelVersion, method="pop", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="wait", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="sync", method_factory=safe_function(None))
    override_method(obj=ModelVersion, method="get_root_object", method_factory=safe_function(None))


def override_project() -> None:
    override_method(obj=Project, method="__getitem__", method_factory=safe_function(Handler(None, None)))
    override_method(obj=Project, method="__setitem__", method_factory=safe_function(None))
    override_method(obj=Project, method="assign", method_factory=safe_function(None))
    override_method(obj=Project, method="fetch", method_factory=safe_function({}))
    override_method(obj=Project, method="ping", method_factory=safe_function(None))
    override_method(obj=Project, method="start", method_factory=safe_function(None))
    override_method(obj=Project, method="stop", method_factory=safe_function(None))
    override_method(obj=Project, method="get_state", method_factory=safe_function(None))
    override_method(obj=Project, method="get_structure", method_factory=safe_function({}))
    override_method(obj=Project, method="print_structure", method_factory=safe_function(None))
    override_method(obj=Project, method="define", method_factory=safe_function(None))
    override_method(obj=Project, method="get_attribute", method_factory=safe_function(None))
    override_method(obj=Project, method="set_attribute", method_factory=safe_function(None))
    override_method(obj=Project, method="exists", method_factory=safe_function(False))
    override_method(obj=Project, method="pop", method_factory=safe_function(None))
    override_method(obj=Project, method="wait", method_factory=safe_function(None))
    override_method(obj=Project, method="sync", method_factory=safe_function(None))
    override_method(obj=Project, method="get_root_object", method_factory=safe_function(None))
    override_method(obj=Project, method="get_url", method_factory=safe_function(None))
    override_method(obj=Project, method="fetch_runs_table", method_factory=safe_function(None))
    override_method(obj=Project, method="fetch_models_table", method_factory=safe_function(None))


def override_run() -> None:
    override_method(obj=Run, method="__getitem__", method_factory=safe_function(Handler(None, None)))
    override_method(obj=Run, method="__setitem__", method_factory=safe_function(None))
    override_method(obj=Run, method="assign", method_factory=safe_function(None))
    override_method(obj=Run, method="fetch", method_factory=safe_function({}))
    override_method(obj=Run, method="ping", method_factory=safe_function(None))
    override_method(obj=Run, method="start", method_factory=safe_function(None))
    override_method(obj=Run, method="stop", method_factory=safe_function(None))
    override_method(obj=Run, method="get_state", method_factory=safe_function(None))
    override_method(obj=Run, method="get_structure", method_factory=safe_function({}))
    override_method(obj=Run, method="print_structure", method_factory=safe_function(None))
    override_method(obj=Run, method="define", method_factory=safe_function(None))
    override_method(obj=Run, method="get_attribute", method_factory=safe_function(None))
    override_method(obj=Run, method="set_attribute", method_factory=safe_function(None))
    override_method(obj=Run, method="exists", method_factory=safe_function(False))
    override_method(obj=Run, method="pop", method_factory=safe_function(None))
    override_method(obj=Run, method="wait", method_factory=safe_function(None))
    override_method(obj=Run, method="sync", method_factory=safe_function(None))
    override_method(obj=Run, method="get_root_object", method_factory=safe_function(None))
    override_method(obj=Run, method="get_url", method_factory=safe_function(None))
    override_method(obj=Run, method="monitoring_namespace", method_factory=safe_function(None))


def override_handler() -> None:
    override_method(obj=Handler, method="__getitem__", method_factory=safe_function(None))
    override_method(obj=Handler, method="__setitem__", method_factory=safe_function(None))
    override_method(obj=Handler, method="__getattr__", method_factory=safe_function(None))
    override_method(obj=Handler, method="get_root_object", method_factory=safe_function(None))
    override_method(obj=Handler, method="assign", method_factory=safe_function(None))
    override_method(obj=Handler, method="upload", method_factory=safe_function(None))
    override_method(obj=Handler, method="upload_files", method_factory=safe_function(None))
    override_method(obj=Handler, method="log", method_factory=safe_function(None))
    override_method(obj=Handler, method="append", method_factory=safe_function(None))
    override_method(obj=Handler, method="extend", method_factory=safe_function(None))
    override_method(obj=Handler, method="add", method_factory=safe_function(None))
    override_method(obj=Handler, method="pop", method_factory=safe_function(None))
    override_method(obj=Handler, method="remove", method_factory=safe_function(None))
    override_method(obj=Handler, method="clear", method_factory=safe_function(None))
    override_method(obj=Handler, method="delete_files", method_factory=safe_function(None))
    override_method(obj=Handler, method="download", method_factory=safe_function(None))
    override_method(obj=Handler, method="download_last", method_factory=safe_function(None))
    override_method(obj=Handler, method="fetch_hash", method_factory=safe_function(None))
    override_method(obj=Handler, method="fetch_extension", method_factory=safe_function(None))
    override_method(obj=Handler, method="fetch_files_list", method_factory=safe_function(None))
    override_method(obj=Handler, method="list_fileset_files", method_factory=safe_function(None))
    override_method(obj=Handler, method="track_files", method_factory=safe_function(False))


def safe_function(default_return_value: Any = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Tuple, **kwargs: Dict[str, Any]) -> Any:
            if os.getenv(NEPTUNE_SAFETY_MODE, "false").lower() in ("true", "1", "t"):
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
