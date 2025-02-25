import click
from resonate_redpanda.produce import produce
from resonate_redpanda.consume import consume


@click.group()
def cli() -> None:
    pass  # The group is set up without a default command


cli.add_command(produce)
cli.add_command(consume)


def main() -> None:
    cli()
