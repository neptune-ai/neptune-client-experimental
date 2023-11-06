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
    "MonotonicIncBatchSize",
]


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
