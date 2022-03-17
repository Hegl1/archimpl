"""Console script for mosaic."""
import sys

import click


@click.command()
def main(args=None):
    """Console script for mosaic."""
    click.echo('Replace this message by putting your code into '
               'mosaic.cli.main')
    click.echo('See click documentation at http://click.pocoo.org/')
    return 0


if __name__ == '__main__':
    sys.exit(main())  # pragma: no cover
