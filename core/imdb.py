from os import environ
import logging
from core.cache import Cache
from functools import cache
import re
from typing import NamedTuple
from core.req import R
from core.wiki import WIKI
from core.country import to_alpha_3
from core.util import safe_num, tp_split, safe_str
from urllib.error import HTTPError


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


class Movie(NamedTuple):
    id: str
    title: str
    typ: str
    year: int
    duration: int
    votes: int
    rating: float


class IMDBApi:
    def __init__(self):
        key = environ.get('OMDBAPI_KEY')
        self.__omdbapi = f"http://www.omdbapi.com/?apikey={key}&i="
        self.__active = True

    @cache
    @Cache("cache/ombd/{}.json")
    def __get_from_omdbapi(self, id: str):
        js = R.get_json(self.__omdbapi+id)
        isError = js.get("Error")
        response = js.get("Response")
        if isError:
            logger.warning(f"IMDBApi: {id} = {js['Error']}")
            return None
        if response not in (True, 'True', 'true'):
            logger.warning(f"IMDBApi: {id} Response = {response}")
            return None
        return js

    def get_from_omdbapi(self, id: str):
        if not self.__active:
            return None
        try:
            return self.__get_from_omdbapi(id)
        except HTTPError as e:
            if e.code != 401:
                raise
            logger.critical(f"OMDb desactivado por {e}")
            self.__active = False
        return None

    @cache
    def __get_name(self, p: str) -> str | None:
        url = f"https://www.imdb.com/es-es/name/{p}/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/115.0 Safari/537.36'
        }
        html = R.get_body(url, headers=headers, chances=3)
        if html is None:
            return None
        match = re.search(
            r"<title>(.*?)\s*-\s*IMDb\s*</title>",
            html,
            re.IGNORECASE | re.DOTALL
        )
        if not match:
            logger.warning(f"[KO] {url} NOT TITLE")
            return None
        title = match.group(1).strip()
        if title in ("IMDb, an Amazon company", ''):
            logger.warning(f"[KO] {url} BAD TITLE: {title}")
            return None
        return title

    def get_names(self, *ids):
        id_name = WIKI.get_names(*ids)
        for i in set(ids).difference(id_name.keys()):
            name = self.__get_name(i)
            if name:
                id_name[i] = name
        return id_name

    @cache
    def __scrape(self, url: str):
        if not isinstance(url, str):
            return set()
        url = url.strip()
        if len(url) == 0:
            return set()
        body = R.get_body(url)
        if not isinstance(body, str):
            return set()
        ok = set(re.findall(r"\btt\d+", body))
        logger.debug(f"{len(ok)} ids en {url}")
        return ok

    def scrape(self, *urls: str):
        ids = set()
        for u in urls:
            ids = ids.union(self.__scrape(u))
        return tuple(sorted(ids))

    def get_countries(self, *ids):
        obj = {}
        for i in ids:
            o = self.get_from_omdbapi(i)
            if o is None:
                continue
            ctr = tp_split(r",", o.get('Country'))
            obj[i] = tuple(c for c in map(to_alpha_3, ctr) if c is not None)
        for i, w in WIKI.get_countries(*obj.keys()).items():
            w = tuple(w.split())
            o = obj[i]
            if not o:
                obj[i] = w
                continue
            ok = tuple(set(o).intersection(w))
            obj[i] = ok or (w if len(w) < len(o) else o)
        return obj

    def get(self, id: str):
        obj = self.get_from_omdbapi(id)
        if obj is None:
            return None
        return Movie(
            id=id,
            title=safe_str(obj.get('Title')),
            typ=safe_str(obj.get('Type')),
            year=safe_num(obj.get('Year')),
            duration=safe_num(obj.get('Runtime')),
            votes=safe_num(obj.get('imdbVotes'), default=0),
            rating=safe_num(obj.get('imdbRating'), default=0),
        )


IMDB = IMDBApi()

if __name__ == "__main__":
    import sys
    r = IMDB.get_countries(sys.argv[1])
    print(r)
