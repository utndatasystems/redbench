from .utils import log
import os
import re


def extract_template_from_filepath(filepath):
    assert "imdb/benchmarks" in filepath
    filepath = filepath.split("imdb/benchmarks/")[1]
    benchmark = filepath.split("/")[0]
    template = filepath.split("/")[1]
    if benchmark == "job":
        # Get the number as the template (1, 2, 3, ..., 33)
        return re.match(r"\d+", template).group()
    assert benchmark == "ceb"
    # The folder name is the template (1a, 2a, 2b, ..., 11b)
    return filepath.split("/")[1]


def _is_setup():
    return (
        os.path.exists("imdb/benchmarks")
        and os.path.exists("imdb/benchmarks/job")
        and os.path.exists("imdb/benchmarks/ceb")
    )


def setup_benchmarks(override=False):
    if not override and _is_setup():
        log("IMDb benchmarks already set up.")
        return
    log("Setting up IMDb benchmarks JOB and CEB...")
    os.system("rm -rf imdb/benchmarks")
    os.system("tar -xzf imdb/benchmarks.tar.gz -C imdb/")
