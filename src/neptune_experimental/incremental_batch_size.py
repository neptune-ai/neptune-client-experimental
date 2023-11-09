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

__all__ = [
    "initialize",
]

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

from neptune_experimental.utils import wrap_method

if TYPE_CHECKING:
    from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor


def initialize() -> None:
    from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor

    wrap_method(obj=AsyncOperationProcessor, method="__init__", wrapper=custom_init_async_op_processor)
    wrap_method(obj=AsyncOperationProcessor, method="_check_queue_size", wrapper=custom_check_queue_size)
    wrap_method(obj=AsyncOperationProcessor.ConsumerThread, method="__init__", wrapper=custom_init_consumer_thread)
    wrap_method(obj=AsyncOperationProcessor.ConsumerThread, method="process_batch", wrapper=custom_process_batch)


class MonotonicIncBatchSize:
    def __init__(self, size_limit: int, initial_size: int = 10, scale_coef: float = 1.6):
        if size_limit <= 0 or not isinstance(size_limit, int):
            raise ValueError("Size limit must be a positive integer")
        if scale_coef <= 1:
            raise ValueError("Scale coefficient cannot be smaller than 1")
        if initial_size <= 0 or initial_size > size_limit or not isinstance(initial_size, int):
            raise ValueError(f"Initial size must be an integer in the interval (0, {size_limit}>")

        self._size_limit: int = size_limit
        self._current_size: int = initial_size
        self._scale_coef: float = scale_coef

    def increase(self) -> None:
        self._current_size = min(int(self._current_size * self._scale_coef + 1), self._size_limit)

    def get(self) -> int:
        return self._current_size


def custom_init_async_op_processor(
    self: "AsyncOperationProcessor", *args: Any, original: Callable[..., Any], batch_size: int = 1000, **kwargs: Any
) -> None:
    self._incremental_batch_size = MonotonicIncBatchSize(size_limit=batch_size)
    original(self, *args, batch_size=batch_size, **kwargs)


def custom_check_queue_size(
    self: "AsyncOperationProcessor", *args: Any, original: Callable[..., bool], **kwargs: Any
) -> bool:
    if hasattr(self, "_incremental_batch_size") and self._incremental_batch_size is not None:
        return bool(self._queue.size() > self._incremental_batch_size.get() / 2)
    return original(*args, **kwargs)


def custom_init_consumer_thread(
    self: "AsyncOperationProcessor.ConsumerThread", *args: Any, original: Callable[..., Any], **kwargs: Any
) -> None:
    original(self, *args, **kwargs)
    self._batch_size = self._processor._incremental_batch_size.get()


def custom_process_batch(
    self: "AsyncOperationProcessor.ConsumerThread", *args: Any, original: Callable[..., Any], **kwargs: Any
) -> None:
    original(self, *args, **kwargs)
    if self._processor._incremental_batch_size is not None:
        self._processor._incremental_batch_size.increase()
        self._batch_size = self._processor._incremental_batch_size.get()
