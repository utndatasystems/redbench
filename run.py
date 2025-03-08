from src.utils import get_experiment_db
from src.redbench import Redbench
from src.benchmarks import setup_benchmarks
from src.imdb import setup_imdb


if __name__ == "__main__":
    # Download and setup the IMDb database, JOB and CEB, if not already done
    setup_imdb()
    setup_benchmarks()

    # Run Redbench
    redbench = Redbench()
    redbench.run()
