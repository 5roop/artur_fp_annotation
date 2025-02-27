from pathlib import Path


def extract_utterances(p: str | Path):
    from lxml import etree

    doc = etree.fromstring(Path(p).read_bytes())
    prefix = "{http://www.w3.org/XML/1998/namespace}"
    whens = doc.findall(".//{*}when")
    timeline = {
        when.attrib[f"{prefix}id"]: float(when.attrib.get("interval", "0.0"))
        for when in whens
    }
    us = doc.findall(".//{*}u")

    def format_u(u):
        ident = u.attrib.get(f"{prefix}id", None)
        who = u.attrib.get("who", None)
        try:
            who = who.replace("#", "")
        except:
            pass
        start = u.attrib.get("start", None)
        try:
            start = start.replace("#", "")
        except:
            pass
        end = u.attrib.get("end", None)
        try:
            end = end.replace("#", "")
        except:
            pass
        start_s = timeline.get(start, None)
        end_s = timeline.get(end, None)
        w_count = len(u.findall(".//{*}w"))
        return dict(
            ident=ident,
            who=who,
            start=start,
            end=end,
            start_s=start_s,
            end_s=end_s,
            w_count=w_count,
            path=str(p),
            filename=Path(p).name,
        )

    return [format_u(u) for u in us]


def find_audio(p: str | Path):
    p = Path(p).with_suffix("").with_suffix("").name
    hits = list(Path(".").glob(f"**/{p}*.wav"))
    if len(hits) == 1:
        return str(hits[0])
    elif len(hits) == 0:
        raise AttributeError("No hits found!")
    else:
        raise AttributeError("Multiple hits found!")


def get_starts(s: str):
    import re

    pattern = r".*avd_(\d*\.\d*)-(\d*\.\d*).*"
    start = float(re.match(pattern, s).group(1))
    return start


def get_ends(s: str):
    import re

    pattern = r".*avd_(\d*\.\d*)-(\d*\.\d*).*"
    end = float(re.match(pattern, s).group(2))
    return end
