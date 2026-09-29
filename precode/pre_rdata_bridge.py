"""Reads an R .rdata file (e.g. load_peripherals.R's hive/peripherals.rdata)
into polars DataFrames. pyreadr is the only practical library for this and
its own API only returns pandas -- that's a hard boundary of the third-party
library, not a choice, so convert immediately to polars right here and never
touch pandas again anywhere else."""

import pyreadr
import polars as pl


def load_rdata(path):
    result = pyreadr.read_r(path)
    return {name: pl.from_pandas(df) for name, df in result.items()}
