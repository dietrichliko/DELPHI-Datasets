"""DELPHI-datasets: find duplicate urls"""

import pathlib
import json
import collections
import urllib

import re

import click

DEFAULT_PATH = "json"


@click.command
@click.argument(
    "path",
    default=DEFAULT_PATH,
    type=click.Path(file_okay=False, exists=True, path_type=pathlib.Path),
)
def find_dubs(path: pathlib.Path) -> None:
    uris: dict[str, int] = collections.defaultdict(int)
    for json_path in path.glob("*.json"):
        with open(json_path, "r") as inp:
            for dataset in json.load(inp):
                for file in dataset.get("files",[]):
                    uris[file["uri"]] += 1

    print("Number of uris %d", len(uris))
    for uri, nr in uris.items():
        if nr > 1:
            print(uri, nr)


"""DELPHI-datasets: find exts
"""


@click.command
@click.argument(
    "jpath",
    default=DEFAULT_PATH,
    type=click.Path(file_okay=False, exists=True, path_type=pathlib.Path),
)
def find_exts(jpath: pathlib.Path) -> None:
    re_sl = re.compile(r"\.(\d+)")
    for json_path in jpath.glob("*.json"):
        with open(json_path, "r") as inp:
            for dataset in json.load(inp):
                for file in dataset["files"]:
                    uri = urllib.parse.urlparse(file["uri"])
                    path = pathlib.Path(uri.path)
                    name_parts = path.name.split(".")
                    if name_parts[-1] in [".sl", ".al"]:
                        match = re_sl.match(name_parts[-2])
                        if not match:
                            print(name_parts)
                            continue
                        name = ".".join(name_parts[:-2], match.group(1), name_parts[-1])
                        url = f"{uri.scheme}://{uri.netloc}/{path.with_name(name)}"
                        print(url)
