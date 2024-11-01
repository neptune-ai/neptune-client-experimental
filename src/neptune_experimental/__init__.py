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

raise ImportError("""

The `neptune-experimental` package is deprecated and should not be used.

To use the Neptune fetching API, uninstall `neptune-experimental` and install `neptune-fetcher`:

    pip uninstall -y neptune-experimental; pip install neptune-fetcher

If you're using `uv`:

    uv pip uninstall neptune-experimental; uv pip install neptune-fetcher --prerelease=allow

""")
