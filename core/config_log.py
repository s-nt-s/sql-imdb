
from datetime import date
import logging
from core.filemanager import FM


def config_log(file: str):
    file = FM.resolve_path(file)
    FM.dump(file, date.today().isoformat()+"\n")
    strmHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler(str(file), mode='a')
    strmHandler.setLevel(logging.INFO)
    fileHandler.setLevel(logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            fileHandler,
            strmHandler
        ]
    )
