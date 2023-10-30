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
__all__ = ["CustomRun"]

from typing import Any

import neptune


# That's just a boilerplate code to make sure that the extension is loaded
class CustomRun(neptune.Run):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        print("That's custom class")

        kwargs["capture_hardware_metrics"] = False
        kwargs["capture_stdout"] = False
        kwargs["capture_stderr"] = False
        kwargs["capture_traceback"] = False

        super().__init__(*args, **kwargs)
