import click

from Repository import init_repo, add_repo, commit_repo, log_repo, status_repo, checkout_repo

from pathlib import Path


@click.group()
def cli():
    pass


@cli.command()
def init():
    path = Path.cwd()
    init_repo(path)


@cli.command()
@click.argument('file_name')
def add(file_name):
    path = Path.cwd()
    add_repo(path, file_name)


@cli.command()
@click.argument('message')
def commit(message):
    path = Path.cwd()
    commit_repo(path, message)


@cli.command()
def status():
    path = Path.cwd()
    status_repo(path)


@cli.command()
def log():
    path = Path.cwd()
    log_repo(path)


@cli.command()
@click.argument('version_hash_code')
def checkout(version_hash_code):
    path = Path.cwd()
    checkout_repo(path, version_hash_code)


if __name__ == '__main__':
    cli()
