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
import os
import unittest
from unittest.mock import patch

from neptune import (
    Model,
    ModelVersion,
    Project,
    Run,
)
from neptune.handler import Handler

from neptune_experimental.env import NEPTUNE_SAFETY_MODE
from neptune_experimental.safe_mode import initialize


class TestSafeMode(unittest.TestCase):
    @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
    def test_run_enabled_safe_mode(self):
        with patch("neptune.Run.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.run_methods:
                with patch(f"neptune.Run.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Run was mocked (to raise exception)

                    getattr(Run(), method)()

    def test_run_disabled_safe_mode(self):
        with patch("neptune.Run.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.run_methods:
                with patch(f"neptune.Run.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Run was mocked (to raise exception)

                    with self.assertRaises(NotImplementedError):
                        getattr(Run(), method)()

    @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
    def test_project_enabled_safe_mode(self):
        with patch("neptune.Project.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.project_methods:
                with patch(f"neptune.Project.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Project was mocked (to raise exception)

                    getattr(Project(), method)()

    def test_project_disabled_safe_mode(self):
        with patch("neptune.Project.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.project_methods:
                with patch(f"neptune.Project.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Project was mocked (to raise exception)

                    with self.assertRaises(NotImplementedError):
                        getattr(Project(), method)()

    @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
    def test_model_enabled_safe_mode(self):
        with patch("neptune.Model.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.model_methods:
                with patch(f"neptune.Model.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Model was mocked (to raise exception)

                    getattr(Model(), method)()

    def test_model_disabled_safe_mode(self):
        with patch("neptune.Model.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.model_methods:
                with patch(f"neptune.Model.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as Model was mocked (to raise exception)

                    with self.assertRaises(NotImplementedError):
                        getattr(Model(), method)()

    @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
    def test_modelversion_enabled_safe_mode(self):
        with patch("neptune.ModelVersion.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.model_version_methods:
                with patch(f"neptune.ModelVersion.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as ModelVersion was mocked (to raise exception)

                    getattr(ModelVersion(), method)()

    def test_modelversion_disabled_safe_mode(self):
        with patch("neptune.ModelVersion.__init__") as mocked_init:
            mocked_init.return_value = None

            for method in self.model_version_methods:
                with patch(f"neptune.ModelVersion.{method}") as mocked_method:
                    mocked_method.side_effect = NotImplementedError()
                    initialize()  # reinitialize as ModelVersion was mocked (to raise exception)

                    with self.assertRaises(NotImplementedError):
                        getattr(ModelVersion(), method)()

    @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
    def test_handler_enabled_safe_mode(self):
        for method in self.handler_methods:
            with patch(f"neptune.handler.Handler.{method}") as mocked_method:
                mocked_method.side_effect = NotImplementedError()
                initialize()  # reinitialize as Handler was mocked (to raise exception)

                getattr(Handler(None, None), method)()

    def test_handler_disabled_safe_mode(self):
        for method in self.handler_methods:
            with patch(f"neptune.handler.Handler.{method}") as mocked_method:
                mocked_method.side_effect = NotImplementedError()
                initialize()  # reinitialize as Handler was mocked (to raise exception)

                with self.assertRaises(NotImplementedError):
                    getattr(Handler(None, None), method)()

    @property
    def run_methods(self):
        return [
            "__getitem__",
            "__setitem__",
            "assign",
            "fetch",
            "ping",
            "start",
            "stop",
            "get_state",
            "get_structure",
            "print_structure",
            "define",
            "get_attribute",
            "set_attribute",
            "exists",
            "pop",
            "wait",
            "sync",
            "get_root_object",
            "get_url",
            "monitoring_namespace",
        ]

    @property
    def project_methods(self):
        return [
            "__getitem__",
            "__setitem__",
            "assign",
            "fetch",
            "ping",
            "start",
            "stop",
            "get_state",
            "get_structure",
            "print_structure",
            "define",
            "get_attribute",
            "set_attribute",
            "exists",
            "pop",
            "wait",
            "sync",
            "get_root_object",
            "get_url",
            "fetch_runs_table",
            "fetch_models_table",
        ]

    @property
    def model_methods(self):
        return [
            "__getitem__",
            "__setitem__",
            "assign",
            "fetch",
            "ping",
            "start",
            "stop",
            "get_state",
            "get_structure",
            "print_structure",
            "define",
            "get_attribute",
            "set_attribute",
            "exists",
            "pop",
            "wait",
            "sync",
            "get_root_object",
            "get_url",
            "fetch_model_versions_table",
        ]

    @property
    def model_version_methods(self):
        return [
            "__getitem__",
            "__setitem__",
            "assign",
            "fetch",
            "ping",
            "start",
            "stop",
            "get_state",
            "get_structure",
            "print_structure",
            "define",
            "get_attribute",
            "set_attribute",
            "exists",
            "pop",
            "wait",
            "sync",
            "get_root_object",
        ]

    @property
    def handler_methods(self):
        return [
            "__getitem__",
            "__setitem__",
            "__getattr__",
            "get_root_object",
            "assign",
            "upload",
            "upload_files",
            "log",
            "append",
            "extend",
            "add",
            "pop",
            "remove",
            "clear",
            "delete_files",
            "download",
            "download_last",
            "fetch_hash",
            "fetch_extension",
            "fetch_files_list",
            "list_fileset_files",
            "track_files",
        ]
