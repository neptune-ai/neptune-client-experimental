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
    List,
)

from neptune_experimental.utils import wrap_method

if TYPE_CHECKING:
    from neptune import Run
    from neptune.internal.background_job import BackgroundJob


def initialize() -> None:
    from neptune import Run

    # Monkey patching
    wrap_method(obj=Run, method="__init__", wrapper=init_with_enable_remote_signals)
    wrap_method(obj=Run, method="_get_background_jobs", wrapper=get_background_jobs)


def init_with_enable_remote_signals(self: "Run", *args: Any, original: Callable[..., Any], **kwargs: Any) -> None:
    from neptune.internal.utils import verify_type

    enable_remote_signals = kwargs.pop("enable_remote_signals", None)

    if enable_remote_signals is None:
        self._enable_remote_signals = True  # user did not pass this param in kwargs -> default value
    else:

        verify_type("enable_remote_signals", enable_remote_signals, bool)
        self._enable_remote_signals = enable_remote_signals

    original(self, *args, **kwargs)


def get_background_jobs(self: "Run", original: Callable[..., Any]) -> List["BackgroundJob"]:
    from neptune.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob

    background_jobs: List["BackgroundJob"] = original(self)

    if not self._enable_remote_signals:
        # filter-out websocket job
        background_jobs = list(
            filter(
                lambda x: not isinstance(x, WebsocketSignalsBackgroundJob),
                background_jobs,
            )
        )

    return background_jobs
