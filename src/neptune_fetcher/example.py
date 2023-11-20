from neptune_fetcher import FrozenProject


def main():
    project = FrozenProject(workspace="administrator", project="Aleksander-benchmark")
    project.progress_indicator(True)
    ids = list(map(lambda row: row["sys/id"], project.list_runs()))
    print(ids)

    run = next(project.fetch_frozen_runs(["AL-5017"]))
    run.prefetch(["sys/id", "source_code/entrypoint"])
    run.prefetch(["training/hyper_parameters/lr"])
    # run.prefetch(["charts/chart-0"])  # this will raise an Exception
    print(run._cache)
    print(run["sys/id"].fetch())
    print(run["sys/owner"].fetch())
    print(run["sys/owner"].fetch())
    print(run._cache)
    del run["sys/owner"]
    print(run._cache)
    print(run["sys/owner"].fetch())
    print(run["sys/failed"].fetch())
    print(run["sys/creation_time"].fetch())
    print(run["sys/monitoring_time"].fetch())
    print(run._cache)
    print(run["charts/chart-0"].fetch_values())
    print(run._cache)
    print(run["monitoring/81f175c0/cpu"].fetch_values())

    print(run["source_code/files"].download())
    print(project.fetch_runs_df())
    print(list(run.field_names))


if __name__ == "__main__":
    main()
