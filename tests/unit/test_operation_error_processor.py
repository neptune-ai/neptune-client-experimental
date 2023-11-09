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
from unittest.mock import patch

from neptune.common.exceptions import NeptuneException
from neptune.exceptions import MetadataInconsistency

from neptune_experimental.env import NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS
from neptune_experimental.operation_error_processor import OperationErrorProcessor


class TestOperationsErrorsProcessor:
    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "True"})
    def test_sample_only_repeated_steps(self, capsys):
        processor = OperationErrorProcessor()
        duplicated_errors = [
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: a. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: b. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: c. Invalid point: 2.0"
            ),
        ]

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) not in captured.out
        assert str(duplicated_errors[2]) not in captured.out

    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "True"})
    def test_not_affect_other_errors(self, capsys):
        processor = OperationErrorProcessor()
        duplicated_errors = list(
            [
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
                NeptuneException("General error"),
                MetadataInconsistency("X-coordinates (step) must be strictly increasing for series attribute: a."),
            ]
        )

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) in captured.out
        assert str(duplicated_errors[2]) in captured.out

    @patch.dict(os.environ, {NEPTUNE_SAMPLE_SERIES_STEPS_ERRORS: "False"})
    def test_not_sample_when_disabled(self, capsys):
        processor = OperationErrorProcessor()
        duplicated_errors = [
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: a. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: b. Invalid point: 2.0"
            ),
            MetadataInconsistency(
                "X-coordinates (step) must be strictly increasing for series attribute: c. Invalid point: 2.0"
            ),
        ]

        processor.handle(errors=duplicated_errors)

        captured = capsys.readouterr()
        assert str(duplicated_errors[0]) in captured.out
        assert str(duplicated_errors[1]) in captured.out
        assert str(duplicated_errors[2]) in captured.out
