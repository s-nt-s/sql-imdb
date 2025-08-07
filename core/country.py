from pycountry.db import Country as DBCountry
from pycountry import countries as DBCountries, historic_countries
import logging

logger = logging.getLogger(__name__)


def search_country(name: str):
    if name in (None, '', 'N/A'):
        return None
    c: DBCountry = \
        DBCountries.get(name=name) or \
        DBCountries.get(alpha_3=name) or \
        DBCountries.get(alpha_2=name)
    if c is not None:
        return c
    lw_name = name.lower()
    for c in DBCountries:
        for f in ("name", "official_name", "common_name"):
            if hasattr(c, f):
                value = getattr(c, f)
                if not isinstance(value, str):
                    continue
                if lw_name == value.lower():
                    return c
    for c in historic_countries:
        for f in ("name", "official_name", "common_name"):
            if hasattr(c, f):
                value = getattr(c, f)
                if not isinstance(value, str):
                    continue
                if lw_name == value.lower():
                    return c
    alias = {
        "russia": "Russian Federation",
        "turkey": "Türkiye",
        "uk": "GB",
        "yugoslavia": "Yugoslavia, (Socialist) Federal Republic of",
        "occupied palestinian territory": "PSE"
    }.get(lw_name)
    if alias:
        return search_country(alias)
    return None


def _to_alpha_3(s: str):
    if s in (None, '', 'N/A'):
        return None
    if s == "West Germany":
        return _to_alpha_3("Germany")
    if s in ("SUN", "Soviet Union", "Unión soviética", "URSS"):
        return "SUN"
    if s in ("Occupied Palestinian Territory", ):
        return "PSE"
    c = search_country(name=s)
    if c is None:
        raise ValueError(f"País no encontrado: {s}")
    return c.alpha_3.upper()


def to_alpha_3(names: tuple[str, ...]):
    if not isinstance(names, (tuple, list)):
        return tuple()
    arr = []
    for n in names:
        try:
            c = _to_alpha_3(n)
            if c not in arr:
                arr.append(c)
        except ValueError as e:
            logger.critical(str(e))
    return tuple(arr)
