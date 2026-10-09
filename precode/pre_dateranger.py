

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col
import precode.pre_dimchecks as dc
import precode.pre_datashapers as ds



def dfilter(df, datecol, dateStart, dateEnd):
    return df.filter(col(datecol) <= dateEnd).filter(col(datecol) >= dateStart)


def firstdate(df, yearcol, monthcol, outputcolname):
    df_dated = df.withColumn(outputcolname, concat_ws("-",col(yearcol),col(monthcol),lit(1)).cast("date"))
    return df_dated


def appendCal(dataframe, datecol, dtb_date_factors):
    df_match = dataframe.withColumn('date', col(datecol))
    df =df_match.join(dtb_date_factors, on=['date'])
    df.printSchema()
    return df
# sample
# dtb_date_factors = spark.read.parquet('murphy/t3/dtb_date_factors.parquet')
# mtb_gc_form = appendCal(mtb_gc_form, 'gc_started_date', dtb_date_factors)


from datetime import datetime, timedelta

def date_add_days(date_str, n):
    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')

    # Add n days to the date
    new_date_obj = date_obj + timedelta(days=n)

    # Convert the new date object back to a string
    new_date_str = new_date_obj.strftime('%Y-%m-%d')

    return new_date_str


def make_yearmonth(df, timecol):
    dateddf = (df
               .withColumn('year', year(timecol)).withColumn('month', month(timecol))
               .withColumn("monthdate", to_date(concat(lit(col("year")), lit("-"), lit(col("month")), lit("-01"))))
              )
    return dateddf


def maxbusinessdate(df):
    lastdate = df.agg(max('businessdate')).collect()[0][0]
    print('the last date picked is: ')
    print(lastdate)
    df = df.filter(col('businessdate')==lastdate)
    return df



# testers
# df=date_event
# datecol = 'date'

def csv_date_roullete_parse(df, datecol):

  # try_to_timestamp()/try_cast() never throw under spark.sql.ansi.enabled=true
  # (which sparkutils.get_spark() sets), unlike bare cast()/to_date(). Default
  # roulette parser - one format string per candidate shape, tried in dmy -> mdy
  # -> ymd priority order (coalesce takes the first non-null).
  format_order = [
      "yyyy-M-d",   # ISO-ish
      'd/M/yy',     # dmy, 2-digit year
      'd/M/yyyy',   # dmy, 4-digit year
      'M/d/yy',     # mdy, 2-digit year
      'M/d/yyyy',   # mdy, 4-digit year
      'yy/M/d',     # ymd, 2-digit year
      'yyyy/M/d',   # ymd, 4-digit year
  ]
  date_attempts = [try_to_timestamp(col(datecol), lit(f)).cast('date') for f in format_order]

  df_cleaned = df.withColumn(
        "date_cleaned",
        coalesce(col(datecol).try_cast('date'), *date_attempts)
    )
  dc.showcol(df_cleaned, 'date_cleaned')

  dc.nullpcnt(df_cleaned, 'date_cleaned')

  # a null date_cleaned is only a real problem if datecol actually HAD a value -
  # an already-blank input (a not-yet-annotated row, common for an optional
  # field) is expected and fine. Only raise for "had a value, none of the 7
  # formats matched it" - a genuinely garbled/typo'd date worth stopping for.
  real_failures = df_cleaned.filter(col(datecol).isNotNull() & col('date_cleaned').isNull())
  n_real_failures = real_failures.count()
  if n_real_failures > 0:
      dc.showcol(real_failures, datecol)
      raise Exception(f'{n_real_failures} row(s) had a {datecol} value that failed to parse against every format - break!')

  wrong_year = df_cleaned.filter(year(col('date_cleaned'))<=1990)
  if wrong_year.count() != 0:
      dc.showcol(wrong_year, 'date_cleaned')
      raise Exception('year parsed into 0025 formats, break!')

  df_cleaned = (df_cleaned
                .withColumn(datecol, col('date_cleaned'))
                .drop('date_cleaned')
  )

#   df_cleaned = ds.rearrange_to_front(df_cleaned, 'date')


  return df_cleaned


def csv_date_roulette_parse_strict(df, datecol):

  # sparkutils.get_spark sets spark.sql.ansi.enabled=true, under which
  # cast('date') / to_date() THROW on a bad value instead of returning NULL --
  # that kills the coalesce-fallback. So: normalise the known slash formats to
  # an ISO string with pure regex (never throws), then try_cast (returns NULL on
  # failure regardless of ANSI). Verified identical on jvm and sail.
  #   handled: yyyy-M-d (ISO, loose) | d/M/yy | M/d/yy | d/M/yyyy | M/d/yyyy
  #   | yy/M/d | yyyy/M/d
  raw = trim(col(datecol).cast('string'))
  two_digit = r'^(\d{1,2})/(\d{1,2})/(\d{2})$'
  four_digit = r'^(\d{1,2})/(\d{1,2})/(\d{4})$'
  dmy = regexp_replace(regexp_replace(raw, two_digit, r'20$3-$2-$1'), four_digit, r'$3-$2-$1')
  mdy = regexp_replace(regexp_replace(raw, two_digit, r'20$3-$1-$2'), four_digit, r'$3-$1-$2')

  two_digit_ymd = r'^(\d{2})/(\d{1,2})/(\d{1,2})$'
  four_digit_ymd = r'^(\d{4})/(\d{1,2})/(\d{1,2})$'
  ymd = regexp_replace(regexp_replace(raw, two_digit_ymd, r'20$1-$2-$3'), four_digit_ymd, r'$1-$2-$3')

  df_cleaned = (df
      .withColumn('_rl_raw', raw)
      .withColumn('_rl_dmy', dmy)
      .withColumn('_rl_mdy', mdy)
      .withColumn('_rl_ymd', ymd)
      .withColumn('date_cleaned', coalesce(
          expr('try_cast(_rl_raw as date)'),   # already ISO / real date type
          expr('try_cast(_rl_dmy as date)'),   # d/M/y  -- tried first, as before
          expr('try_cast(_rl_mdy as date)'),   # M/d/y  -- fallback
          expr('try_cast(_rl_ymd as date)'),   # y/M/d  -- fallback, same priority as csv_date_roullete_parse
      ))
      .drop('_rl_raw', '_rl_dmy', '_rl_mdy', '_rl_ymd')
  )
  dc.showcol(df_cleaned, 'date_cleaned')

  dc.nullpcnt(df_cleaned, 'date_cleaned')

  # same fix as csv_date_roullete_parse: a null date_cleaned is only a real
  # problem if datecol actually HAD a value - an already-blank input (a
  # not-yet-annotated row, common for an optional field) is expected and fine.
  # Only raise for "had a value, none of the 4 formats matched it".
  real_failures = df_cleaned.filter(col(datecol).isNotNull() & col('date_cleaned').isNull())
  n_real_failures = real_failures.count()
  if n_real_failures > 0:
      dc.showcol(real_failures, datecol)
      raise Exception(f'{n_real_failures} row(s) had a {datecol} value that failed to parse against every format - break!')

  wrong_year = df_cleaned.filter(year(col('date_cleaned'))<=1990)
  if wrong_year.count() != 0:
      dc.showcol(wrong_year, 'date_cleaned')
      raise Exception('year parsed into 0025 formats, break!')

  df_cleaned = (df_cleaned
                .withColumn(datecol, col('date_cleaned'))
                .drop('date_cleaned')
  )

#   df_cleaned = ds.rearrange_to_front(df_cleaned, 'date')


  return df_cleaned

# def maxbusinessdate(adadf):
#     from datetime import datetime, timedelta
#     import pytz
#     nowtime = datetime.now(tz=pytz.timezone("Asia/Singapore"))
#     nowtime

#     n=1
#     condition=True
#     while condition:
#         minus_n = nowtime - timedelta(days=n)
#         minus_n = minus_n.strftime("%Y-%m-%d")
#         minus_n
#         df_businessdate = adadf.filter(col('businessdate')==minus_n)
#         df_businessdate.cache()
#         # each of this takes 20 seconds though! oh if it is zero it takes 1 seconds only
#         tic()
#         nb_rows = df_businessdate.count()
#         condition = nb_rows==0
#         toc()

#         if condition == True:
#             print("still no data on date: ", minus_n)
#             n=n+1
#         else:
#             print("data found on date: ", minus_n)
#             print("with nb_rows = ", nb_rows)
#             break
#     return(df_businessdate)


def shift_monthdate(start_month: str, n: int = -1) -> str:
    from dateutil.relativedelta import relativedelta
    from datetime import date

    shifted = date.fromisoformat(start_month) + relativedelta(months=n)
    monthdate_iso = shifted.isoformat()
    return monthdate_iso
