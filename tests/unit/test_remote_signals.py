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

from neptune import Run
from neptune.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob


def test_disabled_remote_signals():
    with Run(mode="debug", enable_remote_signals=False) as run:
        assert run._enable_remote_signals is False
        jobs = run._get_background_jobs()
        assert not [job for job in jobs if isinstance(job, WebsocketSignalsBackgroundJob)]
