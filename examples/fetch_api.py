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

from neptune import Run

from neptune_fetcher import ReadOnlyProject
from neptune_fetcher.read_only_run import ReadOnlyRun

PROJECT = "<PROJECT HERE>"


def create_neptune_run() -> str:
    with Run(project=PROJECT) as run:
        for i in range(10_000):
            run["series/my_float_series"].log(i**2)
            run["series/my_string_series"].log(f"string no. {i}")

        run["params/lr"] = 0.001
        run.sync()
        return run["sys/id"].fetch()


def main():
    run_id = create_neptune_run()

    print("Run created. Now let's use the new fetcher API")
    project = ReadOnlyProject(project=PROJECT)

    run_info = list(project.list_runs())
    print("Run info list:\n", run_info[:10], "\n###########################################\n")

    run = ReadOnlyRun(read_only_project=project, with_id=run_id)

    # For demonstration purpose only, you should not access this field directly on rely on its existence
    print("Run structure:\n", run._structure, "\n###########################################\n")

    # For demonstration purpose only, you should not access this field directly on rely on its existence
    print("Cache before prefetching:\n", run._cache, "\n###########################################\n")
    run.prefetch(["sys/id", "source_code/entrypoint", "params/lr"])

    # For demonstration purpose only, you should not access this field directly on rely on its existence
    print("Cache after prefetching:\n", run._cache, "\n###########################################\n")

    # Purge local cache
    del run["params/lr"]
    print("Cache after purge:\n", run._cache, "\n###########################################\n")

    # Fetch single field
    lr = run["params/lr"].fetch()
    print("Learning rate:\n", lr, "\n###########################################\n")

    # Fetch series
    series = run["series/my_float_series"].fetch_values()
    last = run["series/my_string_series"].fetch_last()
    print("Float series:\n", series, "\n###########################################\n")
    print("Last item in string series:\n", last, "\n###########################################\n")

    # Fetch runs table
    run_df = project.fetch_runs_df(
        columns=["sys/id", "sys/name", "sys/owner"],
        with_ids=[run["sys/id"] for run in run_info[:10]],
    )
    print("Runs dataframe:\n", run_df, "\n###########################################\n")

    # Run fields
    fields = list(run.field_names)
    print("Run field names:\n", fields, "\n###########################################\n")


if __name__ == "__main__":
    main()
