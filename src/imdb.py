import os
import duckdb


def _is_setup():
    return os.path.exists(IMDB_DB_FILEPATH)


def setup_imdb(duckdb_cli, override=False):
    if not override and _is_setup():
        log("IMDb already set up.")
        return
    os.system(f'[ -f "{IMDB_DB_FILEPATH}" ] && rm "{IMDB_DB_FILEPATH}"')
    log("Downloading and setting up the IMDb database. This may take a few minutes.")
    os.system("wget -q http://event.cwi.nl/da/job/imdb.tgz -O imdb.tgz")
    os.system("mkdir -p imdb/raw_data")
    os.system("tar -xzf imdb.tgz -C imdb/raw_data")
    os.system("rm imdb.tgz")
    os.system(f"{duckdb_cli} imdb/db.duckdb < imdb/schema.sql")
    os.system(f"{duckdb_cli} imdb/db.duckdb < imdb/load.sql")

def setup_tpcds(override=False, target_os="LINUX"):
    tpc_db = duckdb.connect("tpcds/db.duckdb")
    tpc_db.execute("CALL dsdgen(sf=1)")
