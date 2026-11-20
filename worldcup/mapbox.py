"""Public Mapbox configuration shared by the dashboard and presentation check."""

import re
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CDN = "https://api.mapbox.com/mapbox-gl-js/v3.35.0"
STYLE = "mapbox://styles/mapbox/dark-v11"


def public_token(value: str) -> str:
    token = value.strip()
    return token if re.fullmatch(r"pk\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token) else ""


def check_access(value: str, origin: str) -> tuple[bool, str]:
    token = public_token(value)
    if not token:
        return False, "Set MAPBOX_ACCESS_TOKEN to a public pk. token in .env."

    style_path = STYLE.removeprefix("mapbox://styles/")
    style_url = f"https://api.mapbox.com/styles/v1/{style_path}?{urlencode({'access_token': token})}"
    for name, url in (("style", style_url), ("JavaScript", f"{CDN}/mapbox-gl.js"), ("CSS", f"{CDN}/mapbox-gl.css")):
        try:
            with urlopen(Request(url, headers={"Referer": origin}), timeout=5) as response:
                response.read(1)
        except HTTPError as error:
            if name == "style" and error.code == 401:
                return False, "Mapbox rejected the token (401). Use an active public token."
            if name == "style" and error.code == 403:
                return False, f"Mapbox denied the style (403). Check styles:read and the allowed origin {origin}."
            return False, f"Mapbox {name} returned HTTP {error.code}."
        except OSError:
            return False, f"Mapbox {name} is unreachable. Check the network before presenting."
    return True, f"Mapbox style and browser assets are accessible from {origin}."
