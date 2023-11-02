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
from typing import Any

import neptune

from src.neptune_experimental.safe_mode.safe_handler import SafeHandler
from src.neptune_experimental.safe_mode.safe_decorator import safe_function


class SafeRun(neptune.Run):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.__getitem__ = safe_function(SafeHandler(None, None))(self.__getitem__)
        self.__setitem__ = safe_function()(self.__setitem__)
        self.assign = safe_function()(self.assign)
        self.fetch = safe_function({})(self.fetch)
        self.ping = safe_function()(self.ping)
        self.start = safe_function()(self.start)
        self.stop = safe_function()(self.stop)
        self.get_state = safe_function()(self.get_state)
        self.get_structure = safe_function({})(self.get_structure)
        self.print_structure = safe_function()(self.print_structure)
        self.define = safe_function()(self.define)
        self.get_attribute = safe_function()(self.get_attribute)
        self.set_attribute = safe_function()(self.set_attribute)
        self.exists = safe_function(False)(self.exists)
        self.pop = safe_function()(self.pop)
        self.wait = safe_function()(self.wait)
        self.sync = safe_function()(self.sync)
        self.get_root_object = safe_function()(self.get_root_object)
        self.get_url = safe_function()(self.get_url)

        super().__init__(*args, **kwargs)

    @safe_function()
    @property
    def monitoring_namespace(self) -> str:
        return super().monitoring_namespace
