from test.cli.default import run_cli


def test_rm(cleantopo_br):
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "-z",
            "5",
            "-b",
            "169.19251592399996",
            "-90",
            "180",
            "-80.18582802550002",
            "--concurrency",
            "none",
        ]
    )
    out_path = cleantopo_br.dict["output"]["path"] / 5 / 3 / "7.tif"
    assert out_path.exists()
    run_cli(
        [
            "rm",
            cleantopo_br.output_path,
            "-z",
            "5",
            "-b",
            "169.19251592399996",
            "-90",
            "180",
            "-80.18582802550002",
            "-f",
        ]
    )
    assert not out_path.exists()
