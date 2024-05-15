from test.cli.default import run_cli


def test_main():
    # """Main CLI."""
    for command in ["execute", "serve", "cp", "index"]:
        run_cli(
            [command],
            expected_exit_code=2,
            output_contains="Error: Missing argument",
            raise_exc=False,
        )

    run_cli(
        ["invalid_command"],
        expected_exit_code=2,
        output_contains="Error: No such command",
        raise_exc=False,
    )
