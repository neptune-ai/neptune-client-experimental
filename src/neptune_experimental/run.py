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
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.internal.utils import verify_type
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.internal.utils.traceback_job import TracebackJob
from neptune.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob


class CustomRun(neptune.Run):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        enable_remote_signals = kwargs.pop("enable_remote_signals", None)

        if enable_remote_signals is None:
            self._enable_remote_signals = True  # user did not pass this param in kwargs -> default value
        else:

            verify_type("enable_remote_signals", enable_remote_signals, bool)
            self._enable_remote_signals = enable_remote_signals

        super().__init__(*args, **kwargs)

    def _prepare_background_jobs(self) -> BackgroundJobList:
        background_jobs = [PingBackgroundJob()]

        if self._enable_remote_signals:
            websockets_factory = self._backend.websockets_factory(self._project_api_object.id, self._id)
            if websockets_factory:
                background_jobs.append(WebsocketSignalsBackgroundJob(websockets_factory))

        if self._capture_stdout:
            background_jobs.append(StdoutCaptureBackgroundJob(attribute_name=self._stdout_path))

        if self._capture_stderr:
            background_jobs.append(StderrCaptureBackgroundJob(attribute_name=self._stderr_path))

        if self._capture_hardware_metrics:
            background_jobs.append(HardwareMetricReportingJob(attribute_namespace=self._monitoring_namespace))

        if self._capture_traceback:
            background_jobs.append(
                TracebackJob(path=f"{self._monitoring_namespace}/traceback", fail_on_exception=self._fail_on_exception)
            )

        return BackgroundJobList(background_jobs)
