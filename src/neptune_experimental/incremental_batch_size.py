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
from typing import (
    Any,
    Optional,
)

from neptune.internal.operation_processors import (
    async_operation_processor,
    factory,
)
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.partitioned_operation_processor import PartitionedOperationProcessor

from neptune_experimental.env import NEPTUNE_ASYNC_BATCH_SIZE
from neptune_experimental.utils import override


def initialize() -> None:
    override(obj=async_operation_processor, attr="AsyncOperationProcessor", target=CustomAsyncOperationProcessor)
    override(obj=factory, attr="get_operation_processor", target=custom_get_operation_processor)


class MonotonicIncBatchSize:
    def __init__(self, size_limit: int, initial_size: int = 10, scale_coef: float = 1.6):
        assert size_limit > 0
        assert scale_coef > 1
        assert 0 < initial_size <= size_limit

        self._size_limit: int = size_limit
        self._current_size: int = initial_size
        self._scale_coef: float = scale_coef

    def increase(self) -> None:
        self._current_size = min(int(self._current_size * self._scale_coef + 1), self._size_limit)

    def get(self) -> int:
        return self._current_size


class CustomAsyncOperationProcessor(AsyncOperationProcessor):
    class ConsumerThread(AsyncOperationProcessor.ConsumerThread):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._m_batch_size: Optional[MonotonicIncBatchSize] = None

        def _increment(self) -> None:
            if hasattr(self, "_m_batch_size") and self._m_batch_size is not None:
                self._m_batch_size.increase()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._m_batch_size: MonotonicIncBatchSize = MonotonicIncBatchSize(size_limit=self._batch_size)
        self._consumer._m_batch_size = self._m_batch_size

    def _check_queue_size(self) -> bool:
        return bool(self._queue.size() > self._m_batch_size.get() / 2)


def custom_get_operation_processor(*args: Any, **kwargs: Any) -> OperationProcessor:
    processor = factory.get_operation_processor(*args, **kwargs)

    if isinstance(processor, PartitionedOperationProcessor) or isinstance(processor, AsyncOperationProcessor):
        batch_size = int(os.environ.get(NEPTUNE_ASYNC_BATCH_SIZE) or "1000")
        processor.batch_size = batch_size

    return processor
