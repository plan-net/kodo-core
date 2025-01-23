import click

from kodo.service.node import run_service


@click.group()
def cli():
    """CLI for managing kodosumi services."""


@cli.command("service")
@click.argument("loader", required=False)
@click.option("-u", "--url", help="server URL")
@click.option("-C", "--connect", multiple=True,
              help="registry URL to connect to, use multiple times")
@click.option("-o", "--organization", help="organization")
@click.option("-R", "--registry", is_flag=True, default=None, 
              help="registry mode")
@click.option("-f", "--feed", is_flag=True, default=None, 
              help="registry feed mode")
@click.option("-c", "--reset", is_flag=True, default=None, 
              help="reset cache")
@click.option("-d", "--cache-file", help="cache file")
@click.option("-r", "--reload", is_flag=True, default=None, help="reload mode")
@click.option("-y", "--ray", is_flag=True, default=None, help="use ray")
@click.option("-e", "--retry", type=int, help="retry count")
@click.option("-l", "--level", type=click.Choice(
     ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'],
     case_sensitive=False), help="Log output level")
def service(
        loader, url, organization, connect, registry, feed, reset,
        cache_file, reload, level, retry, ray):
    """Start the kodosumi service."""
    run_service(
        loader=loader,
        url=url,
        organization=organization,
        connect=list(connect) if connect else None,
        registry=registry,
        feed=feed,
        cache_data=cache_file,
        cache_reset=reset,
        reload=reload,
        screen_level=level,
        ray=ray,
        retry=retry
    )


if __name__ == "__main__":
    cli()
