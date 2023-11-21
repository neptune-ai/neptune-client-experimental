from neptune import Run

from neptune_fetcher import FrozenProject


def create_neptune_run() -> str:
    with Run(project="Aleksander-benchmark") as run:
        run["files/my_file"].upload("file.json")

        for i in range(10_000):
            run["series/my_float_series"].log(i**2)
            run["series/my_string_series"].log(f"string no. {i}")

        run["params/lr"] = 0.001
        run.sync()
        return run["sys/id"].fetch()


def main():
    run_id = create_neptune_run()

    print("Run created. Now let's use the new fetcher API")
    project = FrozenProject(workspace="administrator", project="Aleksander-benchmark")

    run_info = list(project.list_runs())
    print("Run info list:\n", run_info, "\n###########################################\n")

    run = next(project.fetch_frozen_runs([run_id]))
    print("Run structure:\n", run._structure, "\n###########################################\n")

    print("Cache before prefetching:\n", run._cache, "\n###########################################\n")
    run.prefetch(["sys/id", "source_code/entrypoint", "params/lr"])
    print("Cache after prefetching:\n", run._cache, "\n###########################################\n")

    # purging local cache
    del run["params/lr"]
    print("Cache after purge:\n", run._cache, "\n###########################################\n")

    # Fetching single field
    lr = run["params/lr"].fetch()
    print("Learning rate:\n", lr, "\n###########################################\n")

    # Track progress update
    project.progress_indicator(True)

    # Fetch series
    series = run["series/my_float_series"].fetch_values()
    last = run["series/my_string_series"].fetch_last()
    print("Float series:\n", series, "\n###########################################\n")
    print("Last item in string series:\n", last, "\n###########################################\n")

    # Fetch runs table
    run_df = project.fetch_runs_df(columns=["sys/id", "sys/name", "sys/owner"], with_ids=[run_id])
    print("Runs dataframe:\n", run_df, "\n###########################################\n")

    # Download file
    print("Downloading file")
    run["files/my_file"].download()
    print("File downloaded")

    # Run fields
    fields = list(run.field_names)
    print("Run field names:\n", fields, "\n###########################################\n")


if __name__ == "__main__":
    main()
