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

from typing import Any

import neptune
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.utils import verify_type
from neptune.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob

from neptune_experimental.utils import override


def initialize() -> None:
    # Monkey patching
    neptune.Run.__init__ = override(target=neptune.Run.__init__)(init_with_enable_remote_signals)
    neptune.Run._prepare_background_jobs = override(target=neptune.Run._prepare_background_jobs)(prepare_background_jobs)


def init_with_enable_remote_signals(self, *args: Any, original, **kwargs: Any) -> None:
    print('Hello from "remote_signals" feature!')
    enable_remote_signals = kwargs.pop("enable_remote_signals", None)

    if enable_remote_signals is None:
        self._enable_remote_signals = True  # user did not pass this param in kwargs -> default value
    else:

        verify_type("enable_remote_signals", enable_remote_signals, bool)
        self._enable_remote_signals = enable_remote_signals

    original(self, *args, **kwargs)


def prepare_background_jobs(self, original) -> BackgroundJobList:
    background_jobs = original(self)

    if not self._enable_remote_signals:
        # filter-out websocket job
        background_jobs._jobs = list(
            filter(
                lambda x: not isinstance(x, WebsocketSignalsBackgroundJob),
                background_jobs._jobs,
            )
        )

    return background_jobs
