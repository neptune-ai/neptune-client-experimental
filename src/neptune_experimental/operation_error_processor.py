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

import os
import re
from typing import (
    Any,
    List,
    Set,
)

from neptune.common.exceptions import NeptuneException
from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.utils.logger import logger

from neptune_experimental.env import NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS
from neptune_experimental.utils import override


def initialize() -> None:
    override(obj=AsyncOperationProcessor, attr="ConsumerThread", target=CustomConsumerThread)


class OperationErrorProcessor:
    def __init__(self) -> None:
        self._sampling_enabled = os.getenv(NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS, "false").lower() in ("true", "1", "t")
        self._error_sampling_exp = re.compile(
            r"X-coordinates \(step\) must be strictly increasing for series attribute: (.*)\. Invalid point: (.*)"
        )
        self._logged_steps: Set[str] = set()

    def handle(self, errors: List[NeptuneException]) -> None:
        for error in errors:
            if self._sampling_enabled and isinstance(error, MetadataInconsistency):
                match_exp = self._error_sampling_exp.match(str(error))
                if match_exp:
                    self._handle_not_increased_error_for_step(error, match_exp.group(2))
                    continue

            logger.error("Error occurred during asynchronous operation processing: %s", str(error))

    def _handle_not_increased_error_for_step(self, error: MetadataInconsistency, step: str) -> None:
        if step not in self._logged_steps:
            self._logged_steps.add(step)
            logger.error(
                f"Error occurred during asynchronous operation processing: {str(error)}. "
                + f"Suppressing other errors for step: {step}."
            )


class CustomConsumerThread(AsyncOperationProcessor.ConsumerThread):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._errors_processor: OperationErrorProcessor = OperationErrorProcessor()

    def _handle_errors(self, errors: List[NeptuneException]) -> None:
        self._errors_processor.handle(errors)
