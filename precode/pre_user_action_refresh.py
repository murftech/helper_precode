# importable helper: no reliance on names bleeding in via runpy/exec.
# engine-agnostic -> works the same on sail (spark-connect) and jvm pyspark.
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from precode import pre_dimchecks as dc
from precode import pre_readwrite as rw
from precode import pre_dateranger as dr


##############
# pre wrappers for user refresh
#############

def coltype_comparison (df_a, df_b, prikey):
    # Collect dtypes into dict for quick lookup
    types_a = dict(df_a.dtypes)
    types_b = dict(df_b.dtypes)

    # Build comparison rows
    rows = []
    for target_col in prikey:
        type_a = types_a.get(target_col)
        type_b = types_b.get(target_col)
        rows.append(Row(
            col_name=target_col,
            df_a_type=type_a,
            df_b_type=type_b,
            match_flag=(type_a == type_b)
        ))

    # Create a summary DataFrame
    # pull the session off the input df so this works under both engines
    summary_df = df_a.sparkSession.createDataFrame(rows)
    summary_df.show(truncate=False)

    return summary_df


def timestamp_duplicate(filepath):

    from pathlib import Path
    from datetime import datetime
    import shutil
    import re

    #0. get the file extension
    ext= re.search(r'\..+$', filepath).group(0)

    # 1. Create timestamp suffix

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 2. Build new path with timestamp before extension
    new_path = filepath + '_' + timestamp + ext

    # 3. Copy file
    shutil.copy2(filepath, new_path)

    print("Duplicated file in the same folder to:", new_path)



def refresh_rows_user_actions(newdata, mnl_data, prikey, user_given_fields, refresh_now, datecol, orderby=None, refreshpath=None):
    ### function ###
    fixed_col_order = mnl_data.columns
    mnl_data = mnl_data.orderBy([col(c).desc() for c in prikey])
    mnl_data.show()

    # remove computed fields from refreshdata
    mnl_data_user_only = mnl_data.select(prikey + user_given_fields)
    mnl_data_user_only.show()

    newdata.printSchema()

    print('railguard 1: the join keys needs to match typing')

    typer = coltype_comparison(newdata, mnl_data, prikey)
    typer.show()

    if (typer.filter(~col('match_flag')).count() > 0):
        raise Exception('given typing is not matching')

    if (typer.filter(~col('match_flag')).count() == 0):
        print('join keys type match passed')

    print('railguard 2: here if the joins are nothing, something is terribly wrong.')
    dc.in_a_b(newdata, mnl_data_user_only, prikey)

    newdata_plus_user_fields = newdata.join(mnl_data_user_only, on=prikey, how='left')

    # register the current column order
    print('NOTE: if i require new computed columns, please define them in the csv first')
    refreshed_data = newdata_plus_user_fields.select(fixed_col_order)
    # refreshed_data = refreshed_data.distinct()
    # what is this distinct for
    # i should not distinct it. And if i dont distinct it will i keep having more and more rows?
    

    if orderby==None:
        refreshed_data = refreshed_data.orderBy([col(c).desc() for c in prikey])
    else:
        refreshed_data = refreshed_data.orderBy([col(c).desc() for c in prikey])

    refreshed_data.agg(F.max(datecol)).show()
    refreshed_data.show()
    dc.dim(refreshed_data)

    if refresh_now == 'y':
        if refreshpath is None:
            raise ValueError("refresh_now='y' but no refreshpath given -- pass refreshpath=... so the csv can be written back")
        print('replace = yes given, hence update spark_write_csv done at the same time`')
        timestamp_duplicate(refreshpath)
        rw.spark_write_csv(refreshed_data, refreshpath)

    return refreshed_data


def apply_types(df, schema):
    """Cast annotation columns from csv text to real types, per `schema`
    ({'col': 'int' | 'double' | 'string' | 'date' ...}), just before a write.

    try_cast, not cast: ANSI is on (get_spark), where cast() THROWS on 'x'.
    a value that doesn't convert becomes NULL - and is counted and printed here,
    so a typo in the csv can't disappear silently."""
    for c, t in schema.items():
        if c not in df.columns:
            raise ValueError(f'[schema] {c!r} is in SCHEMA but not in the table - columns: {df.columns}')
        casted = F.expr(f'try_cast(`{c}` as {t})')
        bad = df.filter(col(c).isNotNull() & (F.trim(col(c).cast('string')) != '') & casted.isNull())
        n_bad = bad.count()
        if n_bad:
            examples = [r[0] for r in bad.select(c).distinct().limit(10).collect()]
            print(f'[schema] {c} -> {t}: {n_bad} value(s) did not convert, written as NULL: {examples}')
        df = df.withColumn(c, casted)
    return df


def load_annotations(spark, refreshpath, datecol='date'):
    """Read the user-annotated csv the way every pipe_fun stage did inline."""
    mnl_data = spark.read.csv(refreshpath, header=True).dropDuplicates()
    return dr.csv_date_roullete_parse(mnl_data, datecol)


def refresh_with_annotation_gate(newdata, refreshpath, prikey, user_given_fields, datecol='date'):
    """
    refresh_rows_user_actions + a pause for the human, in one call.

    pass 1  refresh and WRITE the csv back: rows the csv has never seen are
            appended with their user fields blank (same as refresh_now='y').
    gate    whenever someone is at a terminal (new rows or not - you may want to
            edit existing annotations): open the csv, wait for Enter, re-read it.
            repeats while any NEW row is still completely blank; 'c' continues
            without re-reading.
    pass 2  refresh from the re-read csv WITHOUT rewriting it (refresh_now='n'):
            Excel may still have it open, and pass 1 already made the timestamped
            backup.
    no terminal (launchd / cron / a pipe) -> never pauses, returns pass 1 with
    the new rows blank; annotate and re-run later.
    """
    import sys
    import subprocess
    from functools import reduce

    spark = newdata.sparkSession

    # keys the csv has never seen. materialised NOW: pass 1 rewrites the csv, and a
    # lazy plan would re-read the new csv later and find nothing new.
    _new = (newdata.select(prikey)
            .join(load_annotations(spark, refreshpath, datecol).select(prikey), on=prikey, how='left_anti'))
    new_keys = spark.createDataFrame(_new.collect(), _new.schema)
    n_new = new_keys.count()
    print(f'[annotate] {n_new} row(s) not yet in {refreshpath}')

    refreshed = refresh_rows_user_actions(
        newdata=newdata, mnl_data=load_annotations(spark, refreshpath, datecol),
        prikey=prikey, user_given_fields=user_given_fields,
        refresh_now='y', datecol=datecol, refreshpath=refreshpath)

    if not sys.stdin.isatty():
        print('[annotate] no terminal - not pausing. new rows written with blank fields; annotate the csv and re-run')
        return refreshed

    # always open + pause at a terminal, new rows or not - you may want to edit
    # existing annotations. the blank check below only nags about NEW rows.
    all_blank = reduce(lambda a, b: a & b, [col(f).isNull() for f in user_given_fields])
    subprocess.run(['open', refreshpath])
    while True:
        answer = input(f'\n[annotate] {n_new} new row(s) added to the csv. edit/annotate, SAVE AS CSV, '
                       f'then Enter to re-read  (c = continue as is) ... ').strip().lower()
        if answer == 'c':
            print('[annotate] continuing without re-reading')
            return refreshed

        refreshed = refresh_rows_user_actions(
            newdata=newdata, mnl_data=load_annotations(spark, refreshpath, datecol),
            prikey=prikey, user_given_fields=user_given_fields,
            refresh_now='n', datecol=datecol, refreshpath=refreshpath)

        still_blank = refreshed.join(new_keys, on=prikey, how='inner').filter(all_blank).count()
        if still_blank == 0:
            print(f'[annotate] re-read your edits ({n_new} new row(s), none left blank) - continuing')
            return refreshed
        print(f'[annotate] {still_blank} of {n_new} new row(s) still completely blank')

############


