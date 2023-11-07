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

# import os
# from typing import (
#     Any,
#     List,
#     Optional,
# )
#
# import neptune
# from neptune.common.exceptions import NeptuneException
# from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
# from neptune.internal.operation_processors.factory import get_operation_processor
# from neptune.internal.operation_processors.operation_processor import OperationProcessor
# from neptune.internal.operation_processors.partitioned_operation_processor import PartitionedOperationProcessor
#
# from neptune_experimental.env import NEPTUNE_ASYNC_BATCH_SIZE
# from neptune_experimental.incremental_batch_size import MonotonicIncBatchSize
# from neptune_experimental.operation_error_processor import OperationErrorProcessor

#
# def initialize() -> None:
#     neptune.metadata_containers.metadata_container.get_operation_processor = custom_get_operation_processor
#     neptune.internal.operation_processors.factory.AsyncOperationProcessor = CustomAsyncOperationProcessor
#
#
# class CustomAsyncOperationProcessor(AsyncOperationProcessor):
#     class ConsumerThread(AsyncOperationProcessor.ConsumerThread):
#         def __init__(self, *args: Any, **kwargs: Any) -> None:
#             super().__init__(*args, **kwargs)
#             self._m_batch_size: Optional[MonotonicIncBatchSize] = None
#             self._errors_processor: OperationErrorProcessor = OperationErrorProcessor()
#
#         def _handle_errors(self, errors: List[NeptuneException]) -> None:
#             self._errors_processor.handle(errors)
#
#         def process_batch(self, *args: Any, **kwargs: Any) -> None:
#             super().process_batch(*args, **kwargs)
#             if self._m_batch_size is not None:
#                 self._m_batch_size.increase()
#
#     def __init__(self, *args: Any, **kwargs: Any) -> None:
#         super().__init__(*args, **kwargs)
#         self._m_batch_size: MonotonicIncBatchSize = MonotonicIncBatchSize(size_limit=self._batch_size)
#         self._consumer._m_batch_size = self._m_batch_size
#
#     def _check_queue_size(self) -> bool:
#         return bool(self._queue.size() > self._m_batch_size.get() / 2)
#
#
# def custom_get_operation_processor(*args: Any, **kwargs: Any) -> OperationProcessor:
#     processor = get_operation_processor(*args, **kwargs)
#
#     if isinstance(processor, PartitionedOperationProcessor) or isinstance(processor, AsyncOperationProcessor):
#         batch_size = int(os.environ.get(NEPTUNE_ASYNC_BATCH_SIZE) or "1000")
#         processor.batch_size = batch_size
#
#     return processor
