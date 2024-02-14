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
import contextlib
import os
import threading
from pathlib import Path
from queue import Queue
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from neptune.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_DATA_DIRECTORY,
)
from neptune.core.components.abstract import WithResources
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
from neptune.internal.operation_processors.utils import get_container_dir
from neptune.internal.utils.logger import get_logger

if TYPE_CHECKING:
    from neptune.core.components.abstract import Resource
    from neptune.core.components.operation_storage import OperationStorage
    from neptune.internal.operation_processors.operation_logger import (
        ProcessorStopSignal,
        ProcessorStopSignalData,
    )
    from neptune.internal.signals_processing.signals import Signal


_logger = get_logger()

SIGNALS_TO_ACCUMULATE = (
    ProcessorStopSignalType.SUCCESS,
    ProcessorStopSignalType.WAITING_FOR_OPERATIONS,
    ProcessorStopSignalType.STILL_WAITING,
)


def get_container_partition_path(type_dir: str, execution_dir_name: str, partition: int) -> Path:
    neptune_data_dir = Path(os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY))
    return neptune_data_dir / type_dir / (execution_dir_name + "__partition_" + str(partition))


def get_container_path(type_dir: str, execution_dir_name: str) -> Path:
    neptune_data_dir = Path(os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY))
    return neptune_data_dir / type_dir / execution_dir_name


class ProcessorStopSignalHandler:
    def __init__(self, processor_stop_logger: ProcessorStopLogger, num_processors: int) -> None:
        self._logger = processor_stop_logger
        self._active_processors = num_processors
        self._total_op_count = 0
        self._still_waiting_signals: Dict[int, int] = {}

    def handle_connection_interruption(self, signal: "ProcessorStopSignal") -> None:
        self._logger.log_connection_interruption(signal.data.max_reconnect_wait_time)

    def handle_waiting_for_operations(
        self,
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"],
    ) -> None:
        self._total_op_count = sum(
            sig_data.size_remaining for sig_data in signals[ProcessorStopSignalType.WAITING_FOR_OPERATIONS]
        )
        self._logger.log_remaining_operations(size_remaining=self._total_op_count)

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

    def mark_processor_as_completed(self, signal: "ProcessorStopSignal") -> None:
        self._still_waiting_signals.pop(signal.data.processor_id, None)
        self._active_processors -= 1

    def handle_still_waiting(
        self,
        signals: Dict[ProcessorStopSignalType, "List[ProcessorStopSignalData]"],
        waiting_signal: "ProcessorStopSignal",
    ) -> None:
        assert waiting_signal.signal_type == ProcessorStopSignalType.STILL_WAITING

        self._still_waiting_signals[waiting_signal.data.processor_id] = waiting_signal.data.already_synced

        # wait for all active processors to send their still waiting signals
        if len(signals[ProcessorStopSignalType.STILL_WAITING]) != self._active_processors:
            return

        # reset the array to wait for the next 'batch' of still waiting signals
        signals[ProcessorStopSignalType.STILL_WAITING] = []

        synced_by_success_proc = sum(sig_data.already_synced for sig_data in signals[ProcessorStopSignalType.SUCCESS])
        synced_by_waiting_proc = sum([already_synced for already_synced in self._still_waiting_signals.values()])

        total_size_synced = synced_by_success_proc + synced_by_waiting_proc

        total_size_remaining = self._total_op_count - total_size_synced

        self._logger.log_still_waiting(
            size_remaining=total_size_remaining,
            already_synced=total_size_synced,
            already_synced_proc=total_size_synced * 100 / self._total_op_count,
        )


class ProcessorStopEventListener(contextlib.AbstractContextManager):
    def __init__(self, num_processors: int, signal_queue: "Queue[ProcessorStopSignal]") -> None:
        self._num_processors = num_processors
        self._signal_queue = signal_queue
        self._t = threading.Thread(target=self.work)
        self._logger = ProcessorStopLogger(processor_id=0, signal_queue=None, logger=_logger)

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
            signal_type: [] for signal_type in SIGNALS_TO_ACCUMULATE
        }

        handler = ProcessorStopSignalHandler(self._logger, self._num_processors)

        while True:  # only a single failure or full success breaks the loop
            try:
                signal = self._signal_queue.get()

                if signal.signal_type in SIGNALS_TO_ACCUMULATE:
                    signals[signal.signal_type].append(signal.data)

                # log connection interruption
                if signal.signal_type == ProcessorStopSignalType.CONNECTION_INTERRUPTED:
                    handler.handle_connection_interruption(signal=signal)

                # log that we wait for operations
                if len(signals[ProcessorStopSignalType.WAITING_FOR_OPERATIONS]) == self._num_processors:
                    handler.handle_waiting_for_operations(signals=signals)

                # if something went wrong - log failure
                if signal.signal_type in (
                    ProcessorStopSignalType.SYNC_FAILURE,
                    ProcessorStopSignalType.RECONNECT_FAILURE,
                ):
                    handler.handle_failure(signal=signal)
                    return

                # mark processor as completed - don't count its operations in the 'still waiting' count
                if signal.signal_type == ProcessorStopSignalType.SUCCESS:
                    handler.mark_processor_as_completed(signal=signal)

                # if all went well - log success
                if len(signals[ProcessorStopSignalType.SUCCESS]) == self._num_processors:
                    handler.handle_success(signals=signals)
                    return

                # log that we still wait + percentage of already synced operations
                if signal.signal_type == ProcessorStopSignalType.STILL_WAITING:
                    handler.handle_still_waiting(signals=signals, waiting_signal=signal)
            except KeyboardInterrupt:
                return


class PartitionedOperationProcessor(WithResources, OperationProcessor):
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
        self._execution_dir_name = get_container_dir(container_id, container_type)
        self._data_path = get_container_path(ASYNC_DIRECTORY, get_container_dir(container_id, container_type))
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
                data_path=get_container_partition_path(ASYNC_DIRECTORY, self._execution_dir_name, partition_id),
                should_print_logs=False,
            )
            for partition_id in range(partitions)
        ]

    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        processor = self._get_operation_processor(op.path)
        processor.enqueue_operation(op, wait=wait)

    @property
    def operation_storage(self) -> "OperationStorage":
        # This is a bit of a hack as we assume that all processors use the same operation storage
        #   this could make problems when the first processor will be cleaned up earlier than the others
        return self._processors[0].operation_storage

    @property
    def data_path(self) -> Path:
        # This is a bit of a hack as we assume that all processors use the same data path
        #   this could make problems when the first processor will be cleaned up earlier than the others
        # mypy had a problem with the return type of the property, so I had to cast it to Path
        return Path(self._processors[0].data_path)

    @property
    def resources(self) -> Tuple["Resource", ...]:
        return tuple(self._processors)

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

    def start(self) -> None:
        # TODO: Handle exceptions
        for processor in self._processors:
            processor.start()

    def stop(self, seconds: Optional[float] = None) -> None:
        # TODO: Handle exceptions

        signal_queue: "Queue[ProcessorStopSignal]" = Queue()

        threads = [
            threading.Thread(target=processor.stop, args=(seconds, signal_queue)) for processor in self._processors
        ]

        with ProcessorStopEventListener(num_processors=len(self._processors), signal_queue=signal_queue):
            for t in threads:
                t.start()

            for t in threads:
                t.join()
