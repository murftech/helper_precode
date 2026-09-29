"""Python port of precode-r/defaults/pre_dateranger.R's csv_date_roulette_parse
-- defends against a CSV's date column silently changing format after being
opened and resaved in a spreadsheet program (dmy vs mdy vs ymd ambiguity).
Tries each order in the same dmy -> mdy -> ymd priority R's lubridate call
uses, then validates: no new nulls introduced by a parse failure, and no
year <=1990 (catches a 2-digit-year mis-parse, e.g. "0026" read as year 26
instead of 2026).

Operates on an already-Spark DataFrame (the CSV itself is read natively via
spark.read.csv elsewhere, per the no-pandas CSV convention) - a UDF is the
right tool for this row-wise, format-ambiguous parse, not pandas or polars.
"""

from dateutil import parser as dateutil_parser

from pyspark.sql.functions import col, year, udf
from pyspark.sql.types import DateType


def _try_parse(v, dayfirst, yearfirst):
    try:
        return dateutil_parser.parse(v, dayfirst=dayfirst, yearfirst=yearfirst).date()
    except Exception:
        return None


def _roulette_parse_one(v):
    if v is None:
        return None
    for dayfirst, yearfirst in [(True, False), (False, False), (False, True)]:  # dmy, mdy, ymd
        d = _try_parse(v, dayfirst, yearfirst)
        if d is not None:
            return d
    return None


def csv_date_roulette_parse(df, datecol):
    # udf() must be created lazily, after a Spark session exists - creating it
    # at module import time (before get_spark() runs) crashes with
    # SESSION_OR_CONTEXT_NOT_EXISTS.
    roulette_udf = udf(_roulette_parse_one, DateType())
    df_cleaned = df.withColumn('date_cleaned', roulette_udf(col(datecol)))

    null_count = df_cleaned.filter(col(datecol).isNotNull() & col('date_cleaned').isNull()).count()
    if null_count > 0:
        df_cleaned.filter(col('date_cleaned').isNull()).show()
        raise SystemExit(f'date parsed returned {null_count} nulls, break!')

    wrong_year_count = df_cleaned.filter(year(col('date_cleaned')) <= 1990).count()
    if wrong_year_count > 0:
        df_cleaned.filter(year(col('date_cleaned')) <= 1990).select('date_cleaned').show()
        raise SystemExit('year parsed into 00xx formats, break!')

    df_cleaned = df_cleaned.withColumn(datecol, col('date_cleaned')).drop('date_cleaned')
    return df_cleaned
