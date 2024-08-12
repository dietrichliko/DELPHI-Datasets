"""DELPHI Datatsets"""

import logging
import re
import sys

log = logging.getLogger(__name__)


def metadata_from_name(name: str) -> tuple[str, str, str, str, bool]:
    """Guess metadata from nickname.

    Arguments:
        nickname: fatfind nickname

    Returns:
        year of datataking  ('91')
        version of processing ('v91b2')
        channel data stream of MC
        format RAW, DSTO, SHORT, LONG etc
        data or Monte carlo
    """
    #   RAW data
    if m := re.match("rawd([0,9]\d)", name):
        year = get_year(m.group(1))
        proc = ""
        channel = "DATA"
        format = "RAWD"
        data = True
    #   RAW data LEP1 & LEP2
    elif m := re.match("rawd_([1,2])", name):
        if m.group(1) == "1":
            year = ["1991", "1992", "1993", "1994", "1995"]
        else:
            year = ["1997", "1998", "1999", "2000"]
        proc = ""
        channel = "DATA"
        format = "RAW"
        data = True
    #   Simulated RAW data ?
    elif m := re.match("raw_(.*)_.*_.+([a,0,9]\d)_.*_(.)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "DSTO"
        data = False
    #   Simulated RAW data ?
    elif m := re.match("raw_(.*)_.*_.+([a,0,9]\d)_.*", name):
        year = get_year(m.group(2))
        proc = ""
        channel = m.group(1).upper()
        format = "RAW"
        data = False
    #   Cosmic events (by some strange reason)
    elif m := re.match("cosd97(.*)?", name):
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
        format = "RAW"
        data = False
    #   leptonic events DSTO
    elif m := re.match("lept([a,0,9]\d)_(.)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "LEPT"
        format = "DSTO"
        data = True
    #   leptonic events DSTO with processing tag
    elif m := re.match("lept([a,0,9]\d)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), "?")
        channel = "LEPT"
        format = "DSTO"
        data = True
    #   leptonic events long dst
    elif m := re.match("lolept([a,0,9]\d)_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "LOLEPT"
        format = "LDST"
        data = True
    #   longdst from LEP2
    elif m := re.match("long(9\d)(p3)?(_(.*))?", name):
        year = get_year(m.group(1))
        proc = get_processing(year, m.group(4))
        channel = "LONG"
        format = "LONG"
        data = True
    #   short DST LEP1
    elif m := re.match("short([0,9]\d)z?_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "SHORT"
        format = "SDST"
        data = True
    #   xshort DST LEP2
    elif m := re.match("xshort([0,9]\d)z?_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "DATA"
        format = "XSDST"
        data = True
    #   xshort DST LEP2 of all data
    elif m := re.match("alld([0,9]\d)_(e\d\d\d_)?(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "ALLD"
        format = "XSDST"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match("xdst([0,9]\d)z?_.*_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "XDST"
        format = "SDST"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match("hadr([0,9]\d)z?_.*_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "HADR"
        format = "XSDST"
        data = True
    #   xshort DST LEP2 of data
    elif m := re.match("xsdst([0,9]\d)z?_(.*_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "XSDST"
        format = "XSDST"
        data = True
    #   LEP2 stic data
    elif m := re.match("stic([0,9]\d)_(.*_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "STIC"
        format = "XSDST"
        data = True
    #   LEP2 scan data
    elif m := re.match("hadr([0,9]\d)_(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(2))
        channel = "HADR"
        format = "XSDST"
        data = True
    #   LEP2 scan data
    elif m := re.match("scan([0,9]\d)_(e\d\d\d_)?+(..)", name):
        year = get_year(m.group(1))
        proc = get_processing(m.group(1), m.group(3))
        channel = "SCAN"
        format = "XSDST"
        data = True
    #   DST-only data LEP2
    elif m := re.match("dsto([0,9]\d)(z.*|p\d+)?(_(.))?", name):
        year = get_year(m.group(1))
        if m.group(4):
            proc = get_processing(m.group(1), m.group(4))
        else:
            proc = get_processing(m.group(1), "?")
        channel = "DSTO"
        format = "DST"
        data = True
    #   DST-only simulation LEP2
    elif m := re.match("dsto_(.*)_.*([a,0,9]\d)_1l_(.)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "DST"
        data = False
    #   short DST simulation LEP1
    elif m := re.match("sh_([^_]+)_.*([0,9]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "SDST"
        data = False
    #   long DST simulation LEP1
    elif m := re.match("lo_([^_]+)_.*([0,9]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "LDST"
        data = False
    #   xshort DST simulation LEP2
    elif m := re.match("xs_([^_]+)_.*([0,9,a]\d)_.*_(.*)", name):
        year = get_year(m.group(2))
        proc = get_processing(m.group(2), m.group(3))
        channel = m.group(1).upper()
        format = "XSDST"
        data = False
    #   xshort DST simulation LEP2
    elif name.startswith("hzha_") or name.startswith("excal_"):
        year = ["2000"]
        proc = "v00?"
        channel = name.split("_")[0].upper()
        format = "XSDST"
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
