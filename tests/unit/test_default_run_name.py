import neptune


def test_default_run_name():
    with neptune.init_run(mode="debug") as run:
        run.sync()
        sys_id = run["sys/id"].fetch()
        assert sys_id is not None
        assert run["sys/name"].fetch() == sys_id

    with neptune.init_run(name="Something", mode="debug") as run2:
        run2.sync()
        assert run2["sys/name"].fetch() == "Something"
