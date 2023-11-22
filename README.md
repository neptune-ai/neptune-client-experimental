# Neptune Experimental Package

This package consists of experimental features that are not yet ready for production use. The API is subject to change without notice.

# Neptune Fetcher

Neptune Fetcher is a Python package designed for efficient fetching and manipulation of data from Neptune projects and runs. It provides classes and methods for interacting with Neptune data in a read-only manner.

## Installation
```bash
pip install --upgrade neptune neptune-experimental
```

## Usage

### Importing

```python
from neptune_fetcher import (
    ReadOnlyProject, ProgressUpdateHandler
)
```

### Overview of Classes
- `ReadOnlyProject`: A lightweight, read-only class for handling basic project information.
    - _Constructor Parameters_:
        - `project`: Optional string specifying the project name.
        - `workspace`: Optional string specifying the workspace.
        - `api_token`: Optional string for the API token.
        - `proxies`: Optional dictionary for proxy configuration.
    - _Methods_:
        - `list_runs()`: Yields dictionaries with basic information about each run, including `sys/id` and `sys/name`.
        - `fetch_read_only_runs(with_ids: List[str])`: Returns a generator for `ReadOnlyRun` instances for specified run IDs.
        - `fetch_runs()`: Fetches runs as a DataFrame with default columns.
        - `progress_indicator(handler: Union[ProgressUpdateHandler, bool])`: Sets a progress indicator.
        - `fetch_runs_df(columns, with_ids, states, owners, tags, trashed)`: Fetches runs as a DataFrame based on specified filters.

- _`ReadOnlyProject.ReadOnlyRun`_: Represents a single Neptune run with read-only access.
    - _Methods_:
        - `__getitem__(item)`: Accesses a field by its path.
        - `__delitem__(key)`: Removes a field from the local cache.
        - `field_names`: Yields the names of all available fields in the run.
        - `prefetch(paths: List[str])`: Loads values of specified fields into local cache.

- `ProgressUpdateHandler`: Handles feedback presentation during data fetching.
    - _Method Overriding_:
        - `pre_series_fetch(total_series: int, series_limit: int)`: Sets up a progress bar for series fetching.
        - `on_series_fetch(step: int)`: On every step in the series fetching process.
        - `post_series_fetch()`: After series fetching is completed should clean up the resources.
        - `pre_runs_table_fetch()`: Initializes a progress bar for table fetching.
        - `on_runs_table_fetch(step: int)`: On every step in the table fetching process.
        - `post_runs_table_fetch()`: After table fetching is completed should clean up the resources.
        - `pre_download(total_size: int)`: Sets up tracking of download process.
        - `on_download_chunk(chunk: int)`: On every step of the download process.
        - `post_download()`: After the download process is completed should clean up the resources.


## Examples
### Fetching Project Metadata

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
```

### Listing Runs in a Project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
ids = list(map(lambda row: row["sys/id"], project.list_runs()))
```

### Filtering and Processing Runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
df = project.fetch_runs_df()

matches = df["sys/name"].str.match("metrics.*")
ids = df[matches]["sys/id"]
```

### Iterating Over Runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
for run in project.fetch_read_only_runs(with_ids=["PROJ-2"]):
    for field in run.field_names:
        if field.startswith("param"):
            print(run[field].fetch())
        if field.startswith("metric"):
            print(run[field].fetch_values())
```

### Prefetching Values

```python
run.prefetch(["metric1", "metric2"])
print(run["metric1"].fetch(), run["metric2"].fetch())  # This will use the local cache
```

### Purging Local Cache

```python
del run["metric1"]
```

### Custom Progress Indicator

Use the default progress indicator:

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
project.progress_indicator(True)
```

or define your own progress indicator by inheriting from `ProgressUpdateHandler`:

```python
from neptune_fetcher import (
    ProgressUpdateHandler,
    ReadOnlyProject,
)


class MyProgressIndicator(ProgressUpdateHandler):
    def pre_runs_table_fetch(self):
        pass

    def on_runs_table_fetch(self, step: int):
        print(f"Fetching runs table, step {step}")

    def post_runs_table_fetch(self):
        pass


project = ReadOnlyProject("some/project")
project.progress_indicator(MyProgressIndicator())
df = project.fetch_runs_df()
```
Output:
```text
Fetching runs table, step 100
Fetching runs table, step 100
Fetching runs table, step 100
```

Implementation of the default update handler can be found in `src/neptune_fetcher/progress_update_handler`.

### Example
A full example can be found in `examples/fetch_api.py`.

## License

This project is licensed under the Apache License Version 2.0. For more details, see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
