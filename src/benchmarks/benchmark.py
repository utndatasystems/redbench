from abc import ABC, abstractmethod


class Benchmark(ABC):
    def __init__(self, **kwargs):
        self.duckdb_cli = kwargs.get("duckdb_cli", None)
        self.stats_db = kwargs.get("stats_db", None)

    @abstractmethod
    def setup(self, override):
        assert False, "Not implemented"

    @abstractmethod
    def compute_stats(self, db):
        assert False, "Not implemented"

    @abstractmethod
    def get_stats(self):
        assert False, "Not implemented"

    @abstractmethod
    def dump_plots(self):
        assert False, "Not implemented"

    @abstractmethod
    def normalize_num_joins(self, num_joins):
        assert False, "Not implemented"