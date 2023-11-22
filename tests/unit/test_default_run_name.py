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

import neptune
import pytest
from neptune.exceptions import MissingFieldException


def test_default_run_name():
    with neptune.init_run(mode="debug") as run:
        run.sync()
        sys_id = run["sys/id"].fetch()
        assert sys_id is not None
        with pytest.raises(MissingFieldException):
            run["sys/name"].fetch()

    with neptune.init_run(name="Something", mode="debug") as run2:
        run2.sync()
        assert run2["sys/name"].fetch() == "Something"
