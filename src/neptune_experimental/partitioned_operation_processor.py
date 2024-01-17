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
import concurrent.futures
import contextlib
import os
import shutil
import threading
from datetime import datetime
from queue import Queue
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.operation_processors.operation_logger import (
    ProcessorStopLogger,
    ProcessorStopSignalType,
)
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.operation_storage import get_container_dir
from neptune.internal.utils.logger import logger as _logger

if TYPE_CHECKING:
    from neptune.internal.operation_processors.operation_logger import (
        ProcessorStopSignal,
        ProcessorStopSignalData,
    )
    from neptune.internal.signals_processing.signals import Signal


class ProcessorStopSignalHandler:
    def __init__(self, processor_stop_logger: ProcessorStopLogger) -> None:
        self._logger = processor_stop_logger

    def handle_connection_interruption(self, signal: "ProcessorStopSignal") -> None:
        self._logger.log_connection_interruption(signal.data.max_reconnect_wait_time)

    def handle_waiting_for_operations(
        self,
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"],
    ) -> None:
        size_remaining = sum(
            sig_data.size_remaining for sig_data in signals[ProcessorStopSignalType.WAITING_FOR_OPERATIONS]
        )
        self._logger.log_remaining_operations(size_remaining=size_remaining)

        # reset the array to wait for the next 'batch' of wait signals
        signals[ProcessorStopSignalType.WAITING_FOR_OPERATIONS] = []

    def handle_failure(
        self,
        signal: "ProcessorStopSignal",
    ) -> None:
        # log sync failure
        if signal.signal_type == ProcessorStopSignalType.SYNC_FAILURE:
            self._logger.log_sync_failure(signal.data.seconds, signal.data.size_remaining)

        # log reconnect failure
        elif signal.signal_type == ProcessorStopSignalType.RECONNECT_FAILURE:
            self._logger.log_reconnect_failure(signal.data.max_reconnect_wait_time, signal.data.size_remaining)

    def handle_success(
        self,
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"],
    ) -> None:
        ops_synced = sum(sig_data.already_synced for sig_data in signals[ProcessorStopSignalType.SUCCESS])
        self._logger.log_success(ops_synced=ops_synced)

    def handle_still_waiting(
        self,
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"],
    ) -> None:
        total_size_synced = sum(sig_data.already_synced for sig_data in signals[ProcessorStopSignalType.STILL_WAITING])
        total_size_remaining = sum(
            sig_data.size_remaining for sig_data in signals[ProcessorStopSignalType.STILL_WAITING]
        )
        total_operations = sum(
            sig_data.already_synced + sig_data.size_remaining
            for sig_data in signals[ProcessorStopSignalType.STILL_WAITING]
        )
        self._logger.log_still_waiting(
            size_remaining=total_size_remaining,
            already_synced=total_size_synced,
            already_synced_proc=total_size_synced / total_operations * 100,
        )


class ProcessorStopEventListener(contextlib.AbstractContextManager):
    def __init__(self, num_processors: int, signal_queue: "Queue[ProcessorStopSignal]") -> None:
        self._num_processors = num_processors
        self._signal_queue = signal_queue
        self._t = threading.Thread(target=self.work)
        self._logger = ProcessorStopLogger(signal_queue=None, logger=_logger)

    def __enter__(self) -> "ProcessorStopEventListener":
        self._t.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_val is not None:
            _logger.error(exc_val, exc_info=exc_tb)
        self._t.join()

    def work(self) -> None:
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"] = {
            signal_type: [] for signal_type in ProcessorStopSignalType
        }

        handler = ProcessorStopSignalHandler(self._logger)

        while True:  # only a single failure or full success breaks the loop
            signal = self._signal_queue.get()
            signals[signal.signal_type].append(signal.data)

            # log connection interruption
            if signal.signal_type == ProcessorStopSignalType.CONNECTION_INTERRUPTED:
                handler.handle_connection_interruption(signal=signal)

            # log that we wait for operations
            if len(signals[ProcessorStopSignalType.WAITING_FOR_OPERATIONS]) == self._num_processors:
                handler.handle_waiting_for_operations(signals=signals)

            # if something went wrong - log failure
            if signal.signal_type in (ProcessorStopSignalType.SYNC_FAILURE, ProcessorStopSignalType.RECONNECT_FAILURE):
                handler.handle_failure(signal=signal)
                return

            # if all went well - log success
            if len(signals[ProcessorStopSignalType.SUCCESS]) == self._num_processors:
                handler.handle_success(signals=signals)
                return

            # log that we still wait + percentage of already synced operations
            if signal.signal_type == ProcessorStopSignalType.STILL_WAITING:
                handler.handle_still_waiting(signals=signals)


class PartitionedOperationProcessor(OperationProcessor):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        backend: NeptuneBackend,
        lock: threading.RLock,
        queue: "Queue[Signal]",
        batch_size: int,
        sleep_time: float = 5,
        partitions: int = 5,
    ):
        self._data_path = self._init_data_path(container_id, container_type)
        self._partitions = partitions
        self._processors = [
            AsyncOperationProcessor(
                container_id=container_id,
                container_type=container_type,
                backend=backend,
                lock=lock,
                queue=queue,
                sleep_time=sleep_time,
                batch_size=batch_size,
                data_path=self._data_path / f"partition-{partition_id}",
                should_print_logs=False,
            )
            for partition_id in range(partitions)
        ]

    @staticmethod
    def _init_data_path(container_id: "UniqueId", container_type: "ContainerType") -> Any:
        now = datetime.now()
        path_suffix = f"exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}-{os.getpid()}"
        return get_container_dir(ASYNC_DIRECTORY, container_id, container_type, path_suffix)

    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        processor = self._get_operation_processor(op.path)
        processor.enqueue_operation(op, wait=wait)

    def _get_operation_processor(self, path: List[str]) -> OperationProcessor:
        path_hash = hash(tuple(path))
        return self._processors[path_hash % self._partitions]

    def pause(self) -> None:
        for processor in self._processors:
            processor.pause()

    def resume(self) -> None:
        for processor in self._processors:
            processor.resume()

    def wait(self) -> None:
        for processor in self._processors:
            processor.wait()

    def flush(self) -> None:
        for processor in self._processors:
            processor.flush()

    def start(self) -> None:
        # TODO: Handle exceptions
        for processor in self._processors:
            processor.start()

    def stop(self, seconds: Optional[float] = None) -> None:
        # TODO: Handle exceptions

        signal_queue: "Queue[ProcessorStopSignal]" = Queue()
        with ProcessorStopEventListener(num_processors=len(self._processors), signal_queue=signal_queue):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        processor.stop,
                        seconds=seconds,
                        signal_queue=signal_queue,
                    )
                    for processor in self._processors
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        if all(processor._queue.is_empty() for processor in self._processors):
            shutil.rmtree(self._data_path, ignore_errors=True)

        if not os.listdir(self._data_path.parent):
            shutil.rmtree(self._data_path.parent, ignore_errors=True)

    def close(self) -> None:
        for processor in self._processors:
            processor.close()
