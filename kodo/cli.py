import sys
import logging
from pathlib import Path

import click

from kodo.log import logger
from kodo.service.node import DEFAULT_LOADER, run_service


@click.group()
def cli():
    """CLI for managing kodosumi services."""


@cli.command("service")
@click.argument("loader", required=False)
@click.option("-u", "--url", help="server URL")
@click.option("-C", "--connect", multiple=True, 
    help="registry URL to connect to, use multiple times")
@click.option("-o", "--organization", help="organization")
@click.option("-R", "--registry", is_flag=True, 
    help="registry mode", default=False)
@click.option("-f", "--feed", is_flag=True, default=False, 
    help="registry feed mode")
@click.option("-c", "--reset", is_flag=True, default=False, help="reset cache")
@click.option("-d", "--cache-file", help="cache file")
@click.option("-r", "--reload", is_flag=True, default=False, 
    help="reload mode")
@click.option(
    "-l", "--level", type=click.Choice(
        ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'],
        case_sensitive=False), default="INFO", help="Log output level")
def service(
        loader, url, organization, connect, registry, feed, reset, 
        cache_file, reload, level):
    """Start the kodosumi service."""
    run_service(
        loader=loader,
        url=url,
        organization=organization,
        connect=list(connect),
        registry=registry,
        feed=feed,
        cache_data=cache_file,
        cache_reset=reset,
        reload=reload,
        screen_level=level,
    )


if __name__ == "__main__":
    cli()
