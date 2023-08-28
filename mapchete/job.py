"""

    # Config object --> define & validate with pydantic

    # relative output paths are not useful, so raise exception
    if not config.out_path.is_remote(out_path) and not config.out_path.is_absolute():
        raise ValueError(f"process output path must be absolute: {out_path}")

    # Mapchete now will initialize the process and prepare all the tasks required.
    # this means the full task graph will be built during this time
    job = Job(
        config,
        **config.params.items()
        concurrency="dask",
    )

    with dask_cluster(**dask_cluster_setup, dask_specs=dask_specs) as cluster:
        with dask_client(
            dask_cluster_setup=dask_cluster_setup, cluster=cluster
        ) as client:

            job.set_executor_kwargs(dict(dask_client=client))

            with Timer() as timer_job:

                adapt_options = dask_specs.get("adapt_options")
                cluster_adapt(
                    cluster,
                    flavor=dask_cluster_setup.get("flavor"),
                    adapt_options=adapt_options,
                )

                for i, _ in enumerate(job, 1):
                    state = backend_db.job(job_id)["properties"]["state"]
                    if state == "aborting":  # pragma: no cover
                        logger.info(
                            "job %s abort state caught: %s", job_id, state
                        )
                        # By calling the job's cancel method, all pending futures will be cancelled.
                        try:
                            job.cancel()
                        except Exception:
                            # catching possible Exceptions (due to losing scheduler before all futures are
                            # cancelled, etc.) makes sure, the job gets the correct cancelled state
                            pass
                        break
                else:
                    # job finished successfully
                    backend_db.set(
                        job_id,
                        state="done",
                        results={
                            "imagesOutput": {
                                "href": str(job.stac_item_path),
                                "type": "application/json",
                            }
                        },
                    )
"""


class DaskJob:
    pass