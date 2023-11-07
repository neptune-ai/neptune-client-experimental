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

from neptune_experimental.utils import override
from neptune_experimental.env import NEPTUNE_SAFETY_MODE


def initialize() -> None:
    override_run()
    override_project()
    override_model_version()
    override_model()
    override_handler()


def override_model() -> None:
    override(
        obj=Model,
        attr="__getitem__",
        method_factory=safe_function(Handler(None, None)))
    override(obj=Model, attr="__setitem__", method_factory=safe_function(None))
    override(obj=Model, attr="assign", method_factory=safe_function(None))
    override(obj=Model, attr="fetch", method_factory=safe_function({}))
    override(obj=Model, attr="ping", method_factory=safe_function(None))
    override(obj=Model, attr="start", method_factory=safe_function(None))
    override(obj=Model, attr="stop", method_factory=safe_function(None))
    override(obj=Model, attr="get_state", method_factory=safe_function(None))
    override(obj=Model, attr="get_structure", method_factory=safe_function({}))
    override(obj=Model, attr="print_structure", method_factory=safe_function(None))
    override(obj=Model, attr="define", method_factory=safe_function(None))
    override(obj=Model, attr="get_attribute", method_factory=safe_function(None))
    override(obj=Model, attr="set_attribute", method_factory=safe_function(None))
    override(obj=Model, attr="exists", method_factory=safe_function(False))
    override(obj=Model, attr="pop", method_factory=safe_function(None))
    override(obj=Model, attr="wait", method_factory=safe_function(None))
    override(obj=Model, attr="sync", method_factory=safe_function(None))
    override(obj=Model, attr="get_root_object", method_factory=safe_function(None))
    override(obj=Model, attr="get_url", method_factory=safe_function(None))
    override(obj=Model, attr="fetch_model_versions_table", method_factory=safe_function(None))


def override_model_version() -> None:
    override(
        obj=ModelVersion,
        attr="__getitem__",
        method_factory=safe_function(Handler(None, None)))
    override(obj=ModelVersion, attr="__setitem__", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="assign", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="fetch", method_factory=safe_function({}))
    override(obj=ModelVersion, attr="ping", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="start", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="stop", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="get_state", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="get_structure", method_factory=safe_function({}))
    override(obj=ModelVersion, attr="print_structure", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="define", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="get_attribute", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="set_attribute", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="exists", method_factory=safe_function(False))
    override(obj=ModelVersion, attr="pop", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="wait", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="sync", method_factory=safe_function(None))
    override(obj=ModelVersion, attr="get_root_object", method_factory=safe_function(None))


def override_project() -> None:
    override(obj=Project, attr="__getitem__", method_factory=safe_function(Handler(None, None)))
    override(obj=Project, attr="__setitem__", method_factory=safe_function(None))
    override(obj=Project, attr="assign", method_factory=safe_function(None))
    override(obj=Project, attr="fetch", method_factory=safe_function({}))
    override(obj=Project, attr="ping", method_factory=safe_function(None))
    override(obj=Project, attr="start", method_factory=safe_function(None))
    override(obj=Project, attr="stop", method_factory=safe_function(None))
    override(obj=Project, attr="get_state", method_factory=safe_function(None))
    override(obj=Project, attr="get_structure", method_factory=safe_function({}))
    override(obj=Project, attr="print_structure", method_factory=safe_function(None))
    override(obj=Project, attr="define", method_factory=safe_function(None))
    override(obj=Project, attr="get_attribute", method_factory=safe_function(None))
    override(obj=Project, attr="set_attribute", method_factory=safe_function(None))
    override(obj=Project, attr="exists", method_factory=safe_function(False))
    override(obj=Project, attr="pop", method_factory=safe_function(None))
    override(obj=Project, attr="wait", method_factory=safe_function(None))
    override(obj=Project, attr="sync", method_factory=safe_function(None))
    override(obj=Project, attr="get_root_object", method_factory=safe_function(None))
    override(obj=Project, attr="get_url", method_factory=safe_function(None))
    override(obj=Project, attr="fetch_runs_table", method_factory=safe_function(None))
    override(obj=Project, attr="fetch_models_table", method_factory=safe_function(None))


def override_run() -> None:
    override(obj=Run, attr="__getitem__", method_factory=safe_function(Handler(None, None)))
    override(obj=Run, attr="__setitem__", method_factory=safe_function(None))
    override(obj=Run, attr="assign", method_factory=safe_function(None))
    override(obj=Run, attr="fetch", method_factory=safe_function({}))
    override(obj=Run, attr="ping", method_factory=safe_function(None))
    override(obj=Run, attr="start", method_factory=safe_function(None))
    override(obj=Run, attr="stop", method_factory=safe_function(None))
    override(obj=Run, attr="get_state", method_factory=safe_function(None))
    override(obj=Run, attr="get_structure", method_factory=safe_function({}))
    override(obj=Run, attr="print_structure", method_factory=safe_function(None))
    override(obj=Run, attr="define", method_factory=safe_function(None))
    override(obj=Run, attr="get_attribute", method_factory=safe_function(None))
    override(obj=Run, attr="set_attribute", method_factory=safe_function(None))
    override(obj=Run, attr="exists", method_factory=safe_function(False))
    override(obj=Run, attr="pop", method_factory=safe_function(None))
    override(obj=Run, attr="wait", method_factory=safe_function(None))
    override(obj=Run, attr="sync", method_factory=safe_function(None))
    override(obj=Run, attr="get_root_object", method_factory=safe_function(None))
    override(obj=Run, attr="get_url", method_factory=safe_function(None))
    override(obj=Run, attr="monitoring_namespace", method_factory=safe_function(None))


def override_handler() -> None:
    override(obj=Handler, attr="__getitem__", method_factory=safe_function(None))
    override(obj=Handler, attr="__setitem__", method_factory=safe_function(None))
    override(obj=Handler, attr="__getattr__", method_factory=safe_function(None))
    override(obj=Handler, attr="get_root_object", method_factory=safe_function(None))
    override(obj=Handler, attr="assign", method_factory=safe_function(None))
    override(obj=Handler, attr="upload", method_factory=safe_function(None))
    override(obj=Handler, attr="upload_files", method_factory=safe_function(None))
    override(obj=Handler, attr="log", method_factory=safe_function(None))
    override(obj=Handler, attr="append", method_factory=safe_function(None))
    override(obj=Handler, attr="extend", method_factory=safe_function(None))
    override(obj=Handler, attr="add", method_factory=safe_function(None))
    override(obj=Handler, attr="pop", method_factory=safe_function(None))
    override(obj=Handler, attr="remove", method_factory=safe_function(None))
    override(obj=Handler, attr="clear", method_factory=safe_function(None))
    override(obj=Handler, attr="delete_files", method_factory=safe_function(None))
    override(obj=Handler, attr="download", method_factory=safe_function(None))
    override(obj=Handler, attr="download_last", method_factory=safe_function(None))
    override(obj=Handler, attr="fetch_hash", method_factory=safe_function(None))
    override(obj=Handler, attr="fetch_extension", method_factory=safe_function(None))
    override(obj=Handler, attr="fetch_files_list", method_factory=safe_function(None))
    override(obj=Handler, attr="list_fileset_files", method_factory=safe_function(None))
    override(obj=Handler, attr="track_files", method_factory=safe_function(False))


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
