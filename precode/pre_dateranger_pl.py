import polars as pl
from datetime import datetime, timedelta
import precode.pre_dimchecks_pl as dc
import precode.pre_datashapers_pl as ds


def dfilter(df, datecol, dateStart, dateEnd):
    return df.filter((pl.col(datecol) >= dateStart) & (pl.col(datecol) <= dateEnd))


def firstdate(df, yearcol, monthcol, outputcolname):
    return df.with_columns(
        pl.concat_str([
            pl.col(yearcol).cast(pl.Utf8),
            pl.lit('-'),
            pl.col(monthcol).cast(pl.Utf8),
            pl.lit('-01'),
        ]).str.to_date().alias(outputcolname)
    )


def appendCal(dataframe, datecol, dtb_date_factors):
    if datecol != 'date':
        df_match = dataframe.with_columns(pl.col(datecol).alias('date'))
    else:
        df_match = dataframe
    return df_match.join(dtb_date_factors, on='date')


def date_add_days(date_str, n):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    new_date_obj = date_obj + timedelta(days=n)
    return new_date_obj.strftime('%Y-%m-%d')


def make_yearmonth(df, timecol):
    return df.with_columns([
        pl.col(timecol).dt.year().alias('year'),
        pl.col(timecol).dt.month().alias('month'),
        pl.date(pl.col(timecol).dt.year(), pl.col(timecol).dt.month(), 1).alias('monthdate'),
    ])


def maxbusinessdate(df):
    lastdate = df['businessdate'].max()
    print('the last date picked is: ')
    print(lastdate)
    return df.filter(pl.col('businessdate') == lastdate)


def csv_date_roullete_parse(df, datecol):
    formats = ['%Y-%m-%d', '%d/%m/%y', '%m/%d/%y']

    raw = df[datecol].cast(pl.Utf8)
    date_series = None
    for fmt in formats:
        parsed = raw.str.to_date(fmt, strict=False)
        if date_series is None:
            date_series = parsed
        else:
            date_series = pl.when(date_series.is_null()).then(parsed).otherwise(date_series).cast(pl.Date)

    df_cleaned = df.with_columns(date_series.alias('date_cleaned'))
    dc.showcol(df_cleaned, 'date_cleaned')
    dc.nullpcnt(df_cleaned, 'date_cleaned')

    if dc.nullindicator == 1:
        raise Exception('date parsed returned some nulls, break!')

    wrong_year = df_cleaned.filter(pl.col('date_cleaned').dt.year() <= 1990)
    if wrong_year.height != 0:
        dc.showcol(wrong_year, 'date_cleaned')
        raise Exception('year parsed into 0025 formats, break!')

    return df_cleaned.with_columns(pl.col('date_cleaned').alias(datecol)).drop('date_cleaned')


def shift_monthdate(start_month: str, n: int = -1) -> str:
    from dateutil.relativedelta import relativedelta
    from datetime import date
    shifted = date.fromisoformat(start_month) + relativedelta(months=n)
    return shifted.isoformat()
