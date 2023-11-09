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
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

from requests.adapters import (
    DEFAULT_POOLSIZE,
    HTTPAdapter,
)

if TYPE_CHECKING:
    from bravado.requests_client import RequestsClient
    from neptune.internal.operation_processors.operation_processor import OperationProcessor

from neptune_experimental.env import NEPTUNE_ASYNC_PARTITIONS_NUMBER
from neptune_experimental.utils import wrap_method

MAX_POOLSIZE = 128


def initialize() -> None:
    from neptune.internal.backends import hosted_client
    from neptune.internal.operation_processors import factory

    wrap_method(obj=hosted_client, method="_set_pool_size", wrapper=custom_set_pool_size)
    wrap_method(obj=factory, method="build_async_operation_processor", wrapper=custom_build_async_op_processor)


def custom_set_pool_size(http_client: "RequestsClient", original: Callable[["RequestsClient"], None]) -> None:
    partitions = int(os.getenv(NEPTUNE_ASYNC_PARTITIONS_NUMBER, "0"))

    if partitions > DEFAULT_POOLSIZE:
        adapter = HTTPAdapter(pool_connections=partitions, pool_maxsize=min(4 * partitions, MAX_POOLSIZE))
        http_client.session.mount("https://", adapter)
        http_client.session.mount("http://", adapter)


def custom_build_async_op_processor(
    *args: Any, original: Callable[..., "OperationProcessor"], **kwargs: Any
) -> "OperationProcessor":
    from neptune.envs import NEPTUNE_ASYNC_BATCH_SIZE
    from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor

    from neptune_experimental.partitioned_operation_processor import PartitionedOperationProcessor

    batch_size = int(os.environ.get(NEPTUNE_ASYNC_BATCH_SIZE) or "1000")
    partitions = int(os.getenv(NEPTUNE_ASYNC_PARTITIONS_NUMBER, "1"))

    if partitions > 1:
        kwargs["batch_size"] = batch_size
        kwargs["partitions"] = partitions

        return PartitionedOperationProcessor(*args, **kwargs)

    return AsyncOperationProcessor(*args, **kwargs, batch_size=batch_size)
