from pycountry.db import Country as DBCountry
from pycountry import countries as DBCountries


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
                if value and lw_name == value.lower():
                    return c
    alias = {
        "russia": "Russian Federation",
        "turkey": "Türkiye",
        "uk": "GB"
    }.get(lw_name)
    if alias:
        return search_country(alias)
    return None


def to_alpha_3(s: str):
    if s in (None, '', 'N/A'):
        return None
    if s == "West Germany":
        return to_alpha_3("Germany")
    c = search_country(name=s)
    if c is None:
        raise ValueError(f"País no encontrado: {s}")
    return c.alpha_3.upper()
