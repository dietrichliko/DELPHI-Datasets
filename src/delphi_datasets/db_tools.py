"""DELPHI Datasets"""

import asyncio
import logging
import pathlib
import re
import shutil
import sys
from typing import Any

import sqlalchemy
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

import delphi_datasets.model as model
import delphi_datasets.tools as tools

log = logging.getLogger(__name__)

FATFIND = shutil.which("fatfind")
if FATFIND is None:
    log.fatal("fatfind command not found.")
    sys.exit(1)


async def create_datasets(
    engine: sqlalchemy.Engine, recids: dict[str, Any], parallel: int
) -> None:
    sem = asyncio.Semaphore(parallel)
    async with asyncio.TaskGroup() as tg:
        for name, recid in recids.items():
            tg.create_task(create_dataset(engine, sem, name, recid))


async def create_dataset(
    engine: sqlalchemy.Engine, sem: asyncio.Semaphore, name: str, recid: int
) -> None:
    async with sem:
        log.info("Creating dataset %s", name)
        nick, gname, desc, comm, files = await parse_fatfind(name)

    with Session(engine) as session:
        years, version, channel, format, data = tools.metadata_from_name(name)
        dataset_obj = model.Dataset(
            name=name,
            recid=recid,
            version=version,
            channel=channel,
            format=format,
            data=data,
            status=model.Status.OK,
        )
        session.add(dataset_obj)

        for year in years:
            dataset_obj.years.add(model.Year.get_instance(session, year))

        cnt = 0
        for path in files:
            file_obj = model.File.get_instance(session, path)
            if file_obj is None:
                continue
            cnt += 1
            dataset_obj.files.add(file_obj)

        if cnt == 0:
            dataset_obj.status = model.Status.EMPTY
        elif cnt < len(files):
            dataset_obj.status = model.Status.INCOMPLETE

        session.commit()


async def parse_fatfind(nick: str) -> tuple[str, str, str, str, list[pathlib.Path]]:
    proc = await asyncio.create_subprocess_exec(
        FATFIND, nick, stdout=asyncio.subprocess.PIPE
    )

    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        log.fatal("Error from fatfind %s", nick)
        raise RuntimeError
    info = stdout.decode("UTF-8")
    match = re.search(
        (
            r"^\s+NICK\s*\:\s+(.*)\n"
            r"\s+GNAME\s*:\s+(.*)\n"
            r"\s+DESC\s*:\s+(.*)\n"
            r"\s+COMM\s*:\s+(.*)$"
        ),
        info,
        re.M,
    )
    if not match:
        log.fatal("Could not extract metadata for %s", nick)
        sys.exit()

    files: list[pathlib.Path] = []
    for m in re.finditer(r"^\s+(\d+)\s+(.*)\s*$", info, re.M):
        files.append(pathlib.Path(m.group(2)))

    return *match.groups(), files


def find_umbrella_datasets(engine: sqlalchemy.Engine) -> None:
    stmt1 = select(model.Dataset).where(model.Dataset.status != model.Status.EMPTY)
    a1 = model.association_table_1.alias("a1")
    a2 = model.association_table_1.alias("a2")
    d = aliased(model.Dataset)

    with Session(engine) as session:
        for dataset in session.execute(stmt1).scalars():
            stmt2 = (
                select(model.Dataset)
                .join(a1, model.Dataset.id == a1.c.dataset_id)
                .join(a2, a1.c.file_id == a2.c.file_id)
                .join(d, a2.c.dataset_id == d.id)
                .where(d.name == dataset.name)
                .where(model.Dataset.id != d.id)
                .group_by(model.Dataset)
            )
            nr = len(dataset.files)
            max_nr = 0
            tot_nr = 0
            for dataset1 in session.execute(stmt2).scalars():
                nr1 = len(dataset1.files)
                max_nr = max(max_nr, nr1)
                tot_nr += nr1
            if max_nr == 0:
                continue
            if nr > max_nr:
                if nr == tot_nr:
                    print(f"Umbrella dataset {dataset}")
                else:
                    print(f"Incomplete Umbrella dataset {dataset}")
