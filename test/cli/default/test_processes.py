from test.cli.default import run_cli


def test_processes():
    run_cli(["processes"])
    run_cli(["processes", "-n", "mapchete.processes.examples.example_process"])
