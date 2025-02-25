import click
from resonate_redpanda.produce import produce
from resonate_redpanda.run import run


@click.group()
def cli() -> None:
    pass  # The group is set up without a default command


cli.add_command(produce)
cli.add_command(run)


def main() -> None:
    cli()
