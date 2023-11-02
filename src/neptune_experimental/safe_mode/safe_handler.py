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

from neptune.handler import Handler

from src.neptune_experimental.safe_mode.safe_decorator import safe_function


class SafeHandler(Handler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.__getitem__ = safe_function()(self.__getitem__)
        self.__setitem__ = safe_function()(self.__setitem__)
        self.__getattr__ = safe_function()(self.__getattr__)
        self.get_root_object = safe_function()(self.get_root_object)
        self.assign = safe_function()(self.assign)
        self.upload = safe_function()(self.upload)
        self.upload_files = safe_function()(self.upload_files)
        self.log = safe_function()(self.log)
        self.append = safe_function()(self.append)
        self.extend = safe_function()(self.extend)
        self.add = safe_function()(self.add)
        self.pop = safe_function()(self.pop)
        self.remove = safe_function()(self.remove)
        self.clear = safe_function()(self.clear)
        self.delete_files = safe_function()(self.delete_files)
        self.download = safe_function()(self.download)
        self.download_last = safe_function()(self.download_last)
        self.fetch_hash = safe_function()(self.fetch_hash)
        self.fetch_extension = safe_function()(self.fetch_extension)
        self.fetch_files_list = safe_function()(self.fetch_files_list)
        self.list_fileset_files = safe_function()(self.list_fileset_files)
        self.track_files = safe_function()(self.track_files)

        super().__init__(*args, **kwargs)