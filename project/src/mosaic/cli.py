import click
import os
import sys


def _load_tables_from_directory(directory):
    for file in os.listdir(directory):
        # TODO load tables from files in the given directory
        click.echo(file)


def _execute_query_file(file_path):
    # TODO execute query files
    click.echo(f"TODO: Execute query at: {file_path}")
    sys.exit(0)


def _main_loop():
    # TODO create main loop of cli
    pass


@click.command()
@click.option("--data-directory", default="./", help="Directory which contains all tables to load at startup")
@click.option("--query-file", default=None, help="Path to an optional query file to execute")
def main(data_directory, query_file):
    _load_tables_from_directory(data_directory)
    if query_file is not None:
        _execute_query_file(query_file)
    return 0

