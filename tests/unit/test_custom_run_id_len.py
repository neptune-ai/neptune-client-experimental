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
from unittest.mock import (
    Mock,
    patch,
)

from faker import Faker
from neptune import init_run
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock

fake = Faker()


@patch("neptune.metadata_containers.metadata_container.get_backend")
def test_max_length_custom_run_id(get_backend_mock):
    get_backend_mock.return_value = Mock(wraps=NeptuneBackendMock())

    custom_run_id = "".join(fake.random_letters(128))
    with init_run(custom_run_id=custom_run_id) as exp:
        _, kwargs = exp._backend.create_run.call_args
        assert kwargs["custom_run_id"] == custom_run_id
