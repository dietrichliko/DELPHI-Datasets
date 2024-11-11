"""DELPHI Datasets."""

from __future__ import annotations

import asyncio
import csv
import itertools
import json
import logging
import pathlib
import re
import shutil
import sys
import tempfile
from typing import Any
import urllib

import sqlalchemy
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from delphi_datasets import model

log = logging.getLogger(__name__)

FATFIND = shutil.which("fatfind")
if FATFIND is None:
    log.fatal("fatfind command not found.")
    sys.exit(1)

EXTRACT = pathlib.Path(__file__).parent.parent / "extract/extract.exe"
if not EXTRACT.exists():
    log.fatal("extract command not found.")
    sys.exit(1)


async def create_datasets(
    engine: sqlalchemy.Engine,
    recids: dict[str, Any],
    parallel: int,
    limit: int | None,
) -> None:
    sem = asyncio.Semaphore(parallel)
    async with asyncio.TaskGroup() as tg:
        for name, recid in itertools.islice(recids.items(), limit):
            tg.create_task(create_dataset(engine, sem, name, recid))

    with Session(engine) as session:
        nr_datasets = session.execute(
            select(func.count()).select_from(model.Dataset),
        ).first()[0]
        nr_files = session.execute(
            select(func.count()).select_from(model.File),
        ).first()[0]
        log.info("Created %d Datasets with %d Files", nr_datasets, nr_files)


async def create_dataset(
    engine: sqlalchemy.Engine,
    sem: asyncio.Semaphore,
    name: str,
    recid: int,
) -> None:
    async with sem:
        log.info("Creating dataset %s", name)
        nick, gname, desc, comm, files = await parse_fatfind(name)

    tot_datasets = 0
    tot_files = 0
    with Session(engine) as session:
        years, version, channel, format, data = metadata_from_name(name)
        dataset_obj = model.Dataset(
            name=name,
            description=desc,
            recid=recid,
            version=version,
            channel=channel,
            format=format,
            data=data,
            status=model.Status.OK,
        )
        session.add(dataset_obj)
        tot_datasets += 1

        for year in years:
            dataset_obj.years.add(model.Year.get_instance(session, year))

        cnt = 0
        for path in files:
            file_obj = model.File.get_instance(session, path)
            if file_obj is None:
                continue
            cnt += 1
            dataset_obj.files.add(file_obj)
            tot_files += 1

        if cnt == 0:
            dataset_obj.status = model.Status.EMPTY
            log.warning("Dataset %s is empty.", name)
        elif cnt < len(files):
            dataset_obj.status = model.Status.INCOMPLETE
            log.warning("Dataset %s is incomplete.", name)

        session.commit()


async def parse_fatfind(nick: str) -> tuple[str, str, str, str, list[pathlib.Path]]:
    proc = await asyncio.create_subprocess_exec(
        FATFIND,
        nick,
        stdout=asyncio.subprocess.PIPE,
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
        re.MULTILINE,
    )
    if not match:
        log.fatal("Could not extract metadata for %s", nick)
        sys.exit()

    files: list[pathlib.Path] = []
    for m in re.finditer(r"^\s+(\d+)\s+(.*)\s*$", info, re.MULTILINE):
        files.append(pathlib.Path(m.group(2)))

    return *match.groups(), files


def find_umbrella_datasets(engine: sqlalchemy.Engine) -> None:
    stmt1 = select(model.Dataset).where(model.Dataset.status != model.Status.EMPTY)
    a1 = model.association_table_1.alias("a1")
    a2 = model.association_table_1.alias("a2")
    d = aliased(model.Dataset)

    with Session(engine) as session:
        for dataset in session.scalars(stmt1):
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
            lt = len(dataset.name)
            max_nr = 0
            tot_nr = 0
            max_lt = 0
            children = session.scalars(stmt2).all()
            for dataset1 in children:
                nr1 = len(dataset1.files)
                max_nr = max(max_nr, nr1)
                tot_nr += nr1
                max_lt = max(len(dataset1.name),max_lt)
            if max_nr == 0:
                continue
            if nr > max_nr:
                if nr == tot_nr:
                    log.info("Umbrella dataset %s", dataset.name)
                else:
                    log.warning("Incomplete Umbrella dataset %s", dataset.name)
                for dataset1 in children:
                    dataset.children.add(dataset1)
            elif nr == max_nr and lt > max_lt:
                log.info("Identical dataset %s", dataset.name)
                for dataset1 in children:
                    dataset.children.add(dataset1)                
            session.commit()


async def extract_metadata(engine: sqlalchemy.Engine) -> None:
    stmt = select(model.Dataset).where(model.Dataset.status != model.Status.EMPTY)
    sem = asyncio.Semaphore(10)
    with Session(engine) as session:
        async with asyncio.TaskGroup() as tg:
            for dataset in session.execute(stmt).scalars():
                for file in dataset.files:
                    tg.create_task(get_metadata(session, file, sem))
        session.commit()


async def get_metadata(
    session: Session,
    file: model.File,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        with tempfile.TemporaryDirectory() as tempdir:
            with pathlib.Path(tempdir, "PDLINPUT").open("w") as pdlinp:
                pdlinp.write(f"FILE = {file.path}")

            proc = await asyncio.create_subprocess_exec(
                EXTRACT,
                file.path,
                stdout=asyncio.subprocess.PIPE,
                cwd=tempdir,
            )

            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                log.fatal("Error from extact %s", file.path)
                raise RuntimeError
            info = stdout.decode("UTF-8")

            match = re.search(r"\{.*\}", info, re.DOTALL)
            if not match:
                log.error("Not JSON sequence found.")
                return

            metadata = json.loads(match.group(0))
            file.entries = metadata["nrecord"]
            file.events = metadata["nevent"]
            for energy in metadata.get("cmenergy", []):
                energy_obj = model.Energy.get_instance(session, energy)
                file.energies.add(energy_obj)


def list(engine: sqlalchemy.Engine) -> None:
    stmt = select(model.Year).order_by(model.Year.name)
    with Session(engine) as session:
        for year in session.execute(stmt).scalars():
            print(year)
            for dataset in year.datasets:
                if dataset.status == model.Status.EMPTY:
                    continue
                energies = [e.value for e in dataset.energies]
                print(f"{dataset.name}: {energies}")


def norm_uri(file: dict[str, Any]) -> str:
    uri = urllib.parse.urlparse(file["uri"])
    match = re.match(r"(.*)\.(\d+)\.(al|sl)", uri.path)
    if match:
        return f"{uri.scheme}://{uri.netloc}/{match.group(1)}.{match.group(2):>06s}.{match.group(3)}"
    else:
        return file["uri"]


def write_json(
    engine: sqlalchemy.Engine,
    outdir: pathlib.Path,
    channels: pathlib.Path,
) -> None:
    categories: dict[str, Any] = {}
    with open(channels) as inp:
        for d in csv.DictReader(inp):
            if d["group2"]:
                categories[d["channel"]] = {
                    "primary": d["group1"],
                    "secondary": d["group2"],
                }
            else:
                categories[d["channel"]] = {
                    "primary": d["group1"],
                }


    stmt1 = select(model.Year).order_by(model.Year.name)
    with Session(engine) as session:
        for year in session.execute(stmt1).scalars():
            all_datasets: list[dict[str, Any]] = []
            for dataset in year.datasets:
                if dataset.status == model.Status.EMPTY:
                    continue
                if dataset.first_year != year.name:
                    continue
                if dataset.name in [ 'hadr99_e1', 'alld99_e1', 'xs_qqnn_e206.7_f00_1l_s1']:
                    continue
                info = {
                    "accelerator": "CERN-LEP",
                    "collaboration": {"name": "DELPHI"},
                    "collision_information": collision_information(
                        dataset.name, dataset.energies
                    ),
                    "collections": ["DELPHI"],
                    "date_created": [year.name],
                    "date_published": "2024",
                    "distribution": {
                        "formats": [dataset.format],
                        "number_files": len(dataset.files),
                        "number_events": dataset.events,
                        "size": dataset.size,
                    },
                    "experiment": ["DELPHI"],
                    "license": {
                        "attribution": "CC0",
                    },
                    "title": f"DELPHI dataset {dataset.name}",
                    "abstract": {
                        "description": dataset.description,
                    },
                    "publisher": "CERN Open Data Portal",
                    "type": {"primary": "Dataset"},
                    "recid": str(dataset.recid),
                    # "doi": f"10.7483/OPENDATA.DELPHI.FAKE.{nr}",
                    "methodology": methodology(
                        dataset.format,
                        dataset.first_year,
                        dataset.data,
                        dataset.version,
                    ),
                    "usage": usage(dataset.format),
                }

                if dataset.data:
                    info["title"] = dataset.title
                    info["type"]["secondary"] = ["Collision"]
                else:
                    info["title"] = dataset.title
                    info["categories"] = categories[dataset.channel]
                    info["type"]["secondary"] = ["Simulated"]

                if len(dataset.children) == 0:
                    info_files = []
                    for file in dataset.files:
                        info_files.append(
                            {
                                "uri": f"root://eospublic.cern.ch/{file.path}",
                                "size": file.size,
                                "checksum": f"adler32:{file.checksum:08x}",
                            },
                        )
                    info_files.sort(key=norm_uri)
                    info["files"] = info_files
                else:
                    info_relations = []
                    for dataset1 in dataset.children:
                        info_relations.append(
                            {
                                "description": dataset1.description,
                                # "doi": dataset1.doi,
                                "recid": str(dataset1.recid),
                                "title": dataset1.title,
                                "type": "isParentOf",
                            },
                        )

                    info["relations"] = info_relations

                all_datasets.append(info)
                # session.commit()

            log.info("Writing %s", f"DELPHI-datasets-{year.name}.json")
            with open(outdir / f"DELPHI-datasets-{year.name}.json", "w") as out:
                json.dump(all_datasets, out, indent=4)

        # with open(outdir / "recids.json", "w") as out:
        #     json.dump(recid_info, out, indent=4)


def collision_information(name: str, energies: list[str]) -> dict[str, str]:
    """Energy slots for datasets"""
    info = {"type": "e+e-"}
    if not name.startswith("rawd") and len(energies):
        energy = int(energies[0].value)
        if energy >= 89 and energy <= 94:
            info["energy"] = "89-94 GeV"
        elif energy >= 130 and energy <= 140:
            info["energy"] = "130-140 GeV"
        elif energy >= 161 and energy <= 174:
            info["energy"] = "161-174 GeV"
        elif energy >= 181 and energy <= 210 or name == "xs_clsp_e189_w98_1l_a1":
            info["energy"] = "181-210 GeV"
        else:
            log.warning("Dataset %s has unexpected energy %d", name, energy)

    return info


def metadata_from_name(name: str) -> tuple[str, str, str, str, bool]:
    """Guess metadata from nickname.

    Arguments:
    ---------
        nickname: fatfind nickname

    Returns:
    -------
        year of datataking  ('91')
        version of processing ('v91b2')
        channel data stream of MC
        format RAW, DSTO, SHORT, LONG etc
        data or Monte carlo

    """
    #   RAW data
    if m := re.match(r"rawd([0,9]\d)", name):
        year = get_year(m.group(1))
        proc = ""
        channel = "DATA"
        format = "RAWD"
        data = True
    #   RAW data LEP1 & LEP2
    elif m := re.match(r"rawd_([1,2])", name):
        if m.group(1) == "1":
            year = ["1991", "1992", "1993", "1994", "1995"]
        else:
            year = ["1997", "1998", "1999", "2000"]
        proc = ""
        channel = "DATA"
        format = "RAWD"
        data = True
    #   Simulated RAW data ?
    elif m := re.match(r"raw_(.*)_.*_.+([a,0,9]\d)_.*_(.)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "RAWD"
        data = False
    #   Simulated RAW data ?
    elif m := re.match(r"raw_(.*)_.*_.+([a,0,9]\d)_.*", name):
        year = get_year(m.group(2))
        proc = ""
        channel = m.group(1).upper()
        format = "RAWD"
        data = False
    #   Cosmic events (by some strange reason)
    elif m := re.match(r"cosd97(.*)?", name):
        year = ["1997"]
        proc = "v97b"
        channel = "COSD"
        format = "DSTO"
        data = True
    #   Simulation of RAW data
    elif name.startswith("pythia"):
        year = ["2000"]
        proc = ""
        channel = "PYTHIA"
        format = "DSTO"
        data = False
    #   leptonic events DSTO
    elif m := re.match(r"lept([a,0,9]\d)_(.)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "LEPT"
        format = "DSTO"
        data = True
    #   leptonic events DSTO with processing tag
    elif m := re.match(r"lept([a,0,9]\d)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), "?")
        channel = "LEPT"
        format = "DSTO"
        data = True
    #   leptonic events long dst
    elif m := re.match(r"lolept([a,0,9]\d)_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "LOLEPT"
        format = "LONG"
        data = True
    #   longdst from LEP2
    elif m := re.match(r"long(9\d)(p3)?(_(.*))?", name):
        year = get_year(m.group(1))
        proc = get_processing(year, m.group(4))
        channel = "LONG"
        format = "LONG"
        data = True
    #   short DST LEP1
    elif m := re.match(r"short([0,9]\d)z?_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "SHORT"
        format = "SHORT"
        data = True
    #   xshort DST LEP2
    elif m := re.match(r"xshort([0,9]\d)z?_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "DATA"
        format = "XSHORT"
        data = True
    #   xshort DST LEP2 of all data
    elif m := re.match(r"alld([0,9]\d)_(e\d\d\d_)?(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "ALLD"
        format = "XSHORT"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match(r"xdst([0,9]\d)z?_.*_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "XDST"
        format = "XSHORT"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match(r"hadr([0,9]\d)z?_.*_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "HADR"
        format = "XSHORT"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match(r"xsdst([0,9]\d)z?_(.*_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "XSDST"
        format = "XSHORT"
        data = True
    #   LEP2 stic data
    elif m := re.match(r"stic([0,9]\d)_(.*_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "STIC"
        format = "XSHORT"
        data = True
    #   LEP2 scan data
    elif m := re.match(r"hadr([0,9]\d)_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "HADR"
        format = "XSHORT"
        data = True
    #   LEP2 scan data
    elif m := re.match(r"scan([0,9]\d)_(e\d\d\d_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "SCAN"
        format = "XSHORT"
        data = True
    #   DST-only data LEP2
    elif m := re.match(r"dsto([0,9]\d)(z.*|p\d+)?(_(.))?", name):
        year = get_year(m.group(1))
        if m.group(4):
            proc = get_processing(m.group(1), m.group(4))
        else:
            proc = get_processing(m.group(1), "?")
        channel = "DSTO"
        format = "DSTO"
        data = True
    #   DST-only simulation LEP2
    elif m := re.match(r"dsto_(.*)_.*([a,0,9]\d)_1l_(.)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "DSTO"
        data = False
    #   short DST simulation LEP1
    elif m := re.match(r"sh_([^_]+)_.*([0,9]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "SHORT"
        data = False
    #   long DST simulation LEP1
    elif m := re.match(r"lo_([^_]+)_.*([0,9]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "LONG"
        data = False
    #   xshort DST simulation LEP2
    elif m := re.match(r"xs_([^_]+)_.*([0,9,a]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "XSHORT"
        data = False
    #   xshort DST simulation LEP2
    elif name.startswith(r"hzha_") or name.startswith("excal_"):
        year = ["2000"]
        proc = "v00?"
        channel = name.split("_")[0].upper()
        format = "XSHORT"
        data = False
    else:
        log.fatal("Unexpected nickname %s", name)
        sys.exit()

    return year, proc, channel, format, data


def get_year(year: str) -> list[str]:
    if year[0] in ["0", "a"]:
        return [f"200{year[1]}"]
    else:
        return [f"19{year}"]


def get_processing(year: str, version: str) -> str:
    if year[0] == "0":
        return f"va{year[1]}{version}"
    else:
        return f"v{year}{version}"


def methodology(format: str, year: str, data: bool, version: str) -> dict[str, str]:
    """Description of the data."""

    if data:
        description = (
            f"The data was recorded by the DELPHI detector in the year {year}."
        )
    else:
        description = f"The data was simulated by DELSIM for the DELPHI detector configuration of the year {year}."

    if format == "DSTO":
        description += f" It was then reconstruced by the detector reconstuction program DELANA (Version {version})."
    elif format in ["SHORT", "LONG", "XSHORT"]:
        description += (
            " It was then reconstruced by the detector reconstuction program DELANA and the "
            f"physics DST program PXDST (Version {version})."
        )

    return {"description": description}


def usage(format: str) -> dict[str, str | dict[str, str]]:
    """Usage of the data."""

    if format == "RAWD":
        description = "The RAW data is availabe for processing with the event server for visaltion with DELGRA."
        links = [
            {
                "description": "The DELPHI Event Server",
                "url": "/docs/delphi-guide-eventserver",
            },
            {
                "description": "The DELPHI Event Display Manual",
                "url": "/record/80503",
            },
        ]
    elif format == "DSTO":
        description = "The detector data is availabe for visaltion with DELGRA"
        links = [
            {
                "description": "The DELPHI Event Display Manual",
                "url": "/record/80503",
            }
        ]
    elif format in ["SHORT", "LONG", "XSHORT"]:
        description = f"The DST data in the {format} format is availabe for anaysis."
        links = [
            {
                "description": "Getting started with DELPHI data",
                "url": "/docs/delphi-getting-started",
            },
            {
                "description": "DELPHI skeleton analysis framework manual",
                "url": "/record/80502",
            },
        ]
        if format == "SHORT":
            links.append(
                {
                    "description": 'DELPHI "short DST" manual',
                    "url": "/record/80506",
                }
            )
        elif format == "LONG":
            links.append(
                {
                    "description": 'DELPHI "full DST" manuals',
                    "url": "/record/80504",
                }
            )
        elif format == "XSHORT":
            links.append(
                {
                    "description": 'DELPHI "extended short DST" manual',
                    "url": "/record/80505",
                }
            )
    else:
        log.error("Unexpected format %s", format)

    return {
        "description": description,
        "links": links,
    }
