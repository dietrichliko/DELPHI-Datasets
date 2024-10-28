"""DELPHI Datasets"""

import asyncio
import json
import logging
import os
import pathlib
import sys
from typing import Any

import click
from sqlalchemy import create_engine

import delphi_datasets.model as model
import delphi_datasets.tools as tools

USER = os.getlogin()
DEFAULT_DBPATH = f"/afs/cern.ch/work/{USER[0]}/{USER}/delphi-datasets.db"
DEFAULT_CHANNELS_CSVPATH = "data/channels.csv"
DEFAULT_REFID_PATH = "data/recids.json"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y%m%d - %H:%M:%S",
    level=logging.INFO,
)

log = logging.getLogger(__name__)


@click.group()
@click.option("-d", "--debug", is_flag=True, default=False, help="Enable debug logging")
def cli(debug: bool) -> None:
    """Create DELPHI datasets definitions for CERN opendata portal."""
    if debug:
        log.setLevel(logging.DEBUG)


@cli.command()
@click.option(
    "--refid",
    default=DEFAULT_REFID_PATH,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    help="JSON file with nicknames and refid",
)
@click.option(
    "--dbpath",
    metavar="DBPATH",
    default=DEFAULT_DBPATH,
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    help="Path to DB",
)
@click.option(
    "--limit",
    metavar="N",
    type=click.IntRange(0),
    help="limit number of entries for debugging",
)
@click.option(
    "-p",
    "--parallel",
    metavar="PROCS",
    default=10,
    type=click.IntRange(1),
    help="Number of parallel processes",
)
@click.option(
    "-s", "--sql-echo", is_flag=True, default=False, help="Enable DB SQL echo"
)
@click.option("--force", default=False, is_flag=True, help="Overwrite DB")
def create(
    refid: pathlib.Path,
    dbpath: pathlib.Path,
    parallel: int,
    force: bool,
    sql_echo: bool,
    limit: int | None,
) -> None:
    """Create sqlite DB of DELPHI datasets."""
    if dbpath.exists():
        if force:
            dbpath.unlink()
        else:
            log.fatal("Database %s already exists. Use --force to overwrite", dbpath)
            sys.exit(1)

    log.info("Creating Database %s", dbpath)
    engine = create_engine(f"sqlite:///{dbpath}", echo=sql_echo)
    model.Base.metadata.create_all(engine)

    with open(refid, "r") as inp:
        recids: dict[str, Any] = {r["nick"]: r["id"] for r in json.load(inp)}

    asyncio.run(tools.create_datasets(engine, recids, parallel, limit))


@cli.command()
@click.option(
    "--dbpath",
    metavar="DBPATH",
    default=DEFAULT_DBPATH,
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    help="Path to DB",
)
@click.option(
    "-s", "--sql-echo", is_flag=True, default=False, help="Enable DB SQL echo"
)
def umbrella(dbpath: pathlib.Path, sql_echo: bool) -> None:
    """Find Umbrella datasets."""

    engine = create_engine(f"sqlite:///{dbpath}", echo=sql_echo)

    tools.find_umbrella_datasets(engine)


@cli.command()
@click.option(
    "--dbpath",
    metavar="DBPATH",
    default=DEFAULT_DBPATH,
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    help="Path to DB",
)
@click.option(
    "-s", "--sql-echo", is_flag=True, default=False, help="Enable DB SQL echo"
)
def metadata(dbpath: pathlib.Path, sql_echo: bool) -> None:
    """Extract metadata from data files."""

    engine = create_engine(f"sqlite:///{dbpath}", echo=sql_echo)

    asyncio.run(tools.extract_metadata(engine))

@cli.command()
@click.option(
    "--dbpath",
    metavar="DBPATH",
    default=DEFAULT_DBPATH,
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    help="Path to DB",
)
@click.option(
    "-s", "--sql-echo", is_flag=True, default=False, help="Enable DB SQL echo"
)
def list(
    dbpath: pathlib.Path, sql_echo: bool) -> None:
    """Write JSON files for OpenData Portal."""
    engine = create_engine(f"sqlite:///{dbpath}", echo=sql_echo)

    tools.list(engine)

@cli.command()
@click.option(
    "--dbpath",
    metavar="DBPATH",
    default=DEFAULT_DBPATH,
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    help="Path to DB",
)
@click.option(
    "-s", "--sql-echo", is_flag=True, default=False, help="Enable DB SQL echo"
)
@click.option(
    "-o",
    "--outdir",
    metavar="DIR",
    default="json",
    type=click.Path(
        file_okay=False, exists=True, writable=True, path_type=pathlib.Path
    ),
)
@click.option(
    "-c",
    "--channels",
    metavar="CSV",
    default=DEFAULT_CHANNELS_CSVPATH,
    type=click.Path(dir_okay=False, exists=True, path_type=pathlib.Path),
)
def write(
    dbpath: pathlib.Path, sql_echo: bool, outdir: pathlib.Path, channels: pathlib.Path
) -> None:
    """Write JSON files for OpenData Portal."""
    engine = create_engine(f"sqlite:///{dbpath}", echo=sql_echo)

    tools.write_json(engine, outdir, channels)
