"""
negotiation.py — HTTP content negotiation helpers.

Parses the Accept header and serialises a Python object to the
best matching format.  Supported types:

    application/json        (default)
    application/xml
    application/x-yaml

If the client sends no Accept header, or none of its preferences
match, JSON is returned (no 406 — see §D rationale).
"""

import json
import xml.etree.ElementTree as ET
import yaml
from aiohttp import web


SUPPORTED = ["application/json", "application/xml", "application/x-yaml"]


def _parse_accept(header: str) -> list[str]:
    """
    Return media types from the Accept header, sorted by quality value
    (highest q first).  Wildcard entries are ignored.
    """
    types = []
    for part in header.split(","):
        part = part.strip()
        if ";q=" in part:
            media, q = part.rsplit(";q=", 1)
            try:
                quality = float(q.strip())
            except ValueError:
                quality = 1.0
        else:
            media = part
            quality = 1.0
        media = media.strip()
        if media and "*" not in media:
            types.append((media, quality))
    types.sort(key=lambda x: x[1], reverse=True)
    return [m for m, _ in types]


def negotiate(request: web.Request) -> str:
    """Return the best supported media type for the request."""
    header = request.headers.get("Accept", "application/json")
    for media in _parse_accept(header):
        if media in SUPPORTED:
            return media
    return "application/json"


# ── Serialisers ──────────────────────────────────────────────────

def _to_xml_element(name: str, data) -> ET.Element:
    el = ET.Element(name)
    if isinstance(data, dict):
        for k, v in data.items():
            child = _to_xml_element(str(k), v)
            el.append(child)
    elif isinstance(data, list):
        for item in data:
            child = _to_xml_element("item", item)
            el.append(child)
    else:
        el.text = str(data) if data is not None else ""
    return el


def serialise(data, media_type: str, root: str = "response") -> web.Response:
    """
    Serialise `data` (dict or list) to the given media type and return
    an aiohttp Response.
    """
    if media_type == "application/xml":
        root_el = _to_xml_element(root, data)
        body = ET.tostring(root_el, encoding="unicode", xml_declaration=False)
        body = '<?xml version="1.0" encoding="UTF-8"?>\n' + body
        return web.Response(text=body, content_type="application/xml")

    if media_type == "application/x-yaml":
        body = yaml.dump(data, default_flow_style=False, allow_unicode=True)
        return web.Response(text=body, content_type="application/x-yaml")

    # default: JSON
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        content_type="application/json"
    )


def error_response(status: int, message: str, detail: str = "") -> web.Response:
    body = {"error": {"code": status, "message": message, "detail": detail}}
    return web.Response(
        status=status,
        text=json.dumps(body),
        content_type="application/json"
    )
