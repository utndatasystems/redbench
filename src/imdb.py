from .utils import IMDB_DB_FILEPATH, log
import duckdb
import os


def _is_setup():
    return os.path.exists(IMDB_DB_FILEPATH)


def setup_imdb(override=False):
    if not override and _is_setup():
        log("IMDb already set up.")
        return
    os.system(f'[ -f "{IMDB_DB_FILEPATH}" ] && rm "{IMDB_DB_FILEPATH}"')
    log("Downloading and setting up the IMDb database. This may take a few minutes...")
    os.system("wget -q http://event.cwi.nl/da/job/imdb.tgz -O imdb.tgz")
    os.system("mkdir -p imdb/raw_data")
    os.system("tar -xzf imdb.tgz -C imdb/raw_data")
    os.system("rm imdb.tgz")
    os.system("duckdb imdb/db.duckdb < imdb/schema.sql")
    os.system("duckdb imdb/db.duckdb < imdb/load.sql")
