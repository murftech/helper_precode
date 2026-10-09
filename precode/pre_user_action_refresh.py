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



def refresh_rows_user_actions(newdata, annote_data, prikey, orderby=None, join='full', user_given_fields=None,
                              computed_columns_sel='newdata'):
    # computed_columns_sel: which side decides the set of computed columns in the output.
    #   'newdata'    (default): every column newdata has is output; one the csv lacks is added at the
    #                START (leftmost, so it is quickly visible), filled from newdata.
    #   'annote_data': the csv decides - newdata columns the csv lacks are ignored (the old behaviour).
    #                Use this in production once the computed columns are stable and fixed.
    if computed_columns_sel not in ('newdata', 'annote_data'):
        raise ValueError(f"computed_columns_sel must be 'newdata' or 'annote_data', got {computed_columns_sel!r}")

    user_fields_inferred = user_given_fields is None
    if user_fields_inferred:
        user_given_fields = [c for c in annote_data.columns if c not in prikey and c not in newdata.columns]
        # computed side of the same inference (printed at the very end, see the bottom of this function)
        computed_in_csv = [c for c in annote_data.columns if c not in prikey and c in newdata.columns]

    fixed_col_order = annote_data.columns
    new_columns = []
    if computed_columns_sel == 'newdata':
        new_columns = [c for c in newdata.columns if c not in annote_data.columns and c not in prikey]
        fixed_col_order = new_columns + fixed_col_order
    annote_data = annote_data.orderBy([col(c).desc() for c in prikey])
    # annote_data.show()

    # remove computed fields from refreshdata
    annote_data_user_only = annote_data.select(prikey + user_given_fields)
    # annote_data_user_only.show()

    newdata.printSchema()

    print('railguard 1: the join keys needs to match typing')

    typer = coltype_comparison(newdata, annote_data, prikey)
    # typer.show()

    if (typer.filter(~col('match_flag')).count() > 0):
        raise Exception('given typing is not matching')

    if (typer.filter(~col('match_flag')).count() == 0):
        print('join keys type match passed')

    print('railguard 2: here if the joins are nothing, something is terribly wrong.')
    dc.in_a_b(newdata, annote_data_user_only, prikey)

    # join='full' (the default) never drops a prikey annote_data has annotated but
    # newdata no longer produces (e.g. it's outside this run's date scope, or
    # hasn't been re-ingested yet) - the row survives with its user_given_fields
    # intact and every newdata-sourced column NULL. join='left' drops it instead,
    # trusting newdata as authoritative - use this ONLY when the key can
    # legitimately be reassigned/reconciled to a DIFFERENT key across runs (e.g.
    # a swap-matching step upstream), where "kept forever under a dead key" is
    # worse than "dropped, and a separate content-based guard catches real loss."
    if join == 'full':
        print('railguard 3 (full join only): prikey cardinality must not change across the join')
        before_cardinality = annote_data.groupBy(*prikey).count().withColumnRenamed('count', 'n_before')

    newdata_plus_user_fields = newdata.join(annote_data_user_only, on=prikey, how=join)

    if join == 'full':
        after_cardinality = newdata_plus_user_fields.groupBy(*prikey).count().withColumnRenamed('count', 'n_after')
        cardinality_check = before_cardinality.join(after_cardinality, on=prikey, how='left') \
            .withColumn('delta', col('n_after') - col('n_before'))
        n_victims = cardinality_check.filter(col('delta') != 0).count()
        if n_victims > 0:
            cardinality_check.filter(col('delta') != 0).show(20, truncate=False)
            raise Exception('[full join] prikey cardinality changed across the join - likely duplication, investigate before proceeding')

    # register the current column order
    if computed_columns_sel == 'annote_data':
        print('NOTE: computed_columns_sel is annote_data: a new computed column has to be defined in the csv first')
    refreshed_data = newdata_plus_user_fields.select(fixed_col_order)
    # refreshed_data = refreshed_data.distinct()
    # what is this distinct for
    # i should not distinct it. And if i dont distinct it will i keep having more and more rows?

    if join == 'full':
        print('railguard 4 (full join only): every existing prikey user-annotation must survive unchanged')
        # a prikey can have MULTIPLE rows (e.g. one bank transaction itemized into
        # several annotation rows sharing the same key) - a plain row-level compare
        # fans out and false-positives on multi-row keys. Fix: collect the sorted
        # multiset of row-signatures per prikey on each side, compare multisets -
        # order-independent, only flags a REAL difference.
        def _row_signature(df):
            parts = [F.coalesce(col(c).cast('string'), F.lit('\x00NULL\x00')) for c in user_given_fields]
            return df.withColumn('_sig', F.concat_ws('\x1f', *parts))

        before_sig = (_row_signature(annote_data_user_only)
                      .groupBy(*prikey).agg(F.sort_array(F.collect_list('_sig')).alias('before_sigs')))
        after_sig = (_row_signature(refreshed_data)
                     .groupBy(*prikey).agg(F.sort_array(F.collect_list('_sig')).alias('after_sigs')))

        compare = before_sig.join(after_sig, on=prikey, how='left')

        missing = compare.filter(col('after_sigs').isNull())
        n_missing = missing.count()

        changed = compare.filter(col('after_sigs').isNotNull() & (col('before_sigs') != col('after_sigs')))
        n_changed = changed.count()

        if n_missing > 0 or n_changed > 0:
            print(f'{n_missing} existing prikey(s) vanished entirely from the output:')
            missing.show(20, truncate=False)
            print(f'{n_changed} existing prikey(s) have a changed set of user-annotation rows:')
            changed.show(20, truncate=False)
            raise Exception('[full join] existing user annotations were altered/lost - investigate before writing')

    if orderby==None:
        refreshed_data = refreshed_data.orderBy([col(c).desc() for c in prikey])
    else:
        refreshed_data = refreshed_data.orderBy(orderby)

    # refreshed_data.show()
    dc.dim(refreshed_data)

    # log-only, LAST thing printed so it is not lost above the guard output: the default is an
    # inference, so show what it decided. A csv column missing from newdata lands on the USER
    # side (kept as typed); a column present in both lands on the COMPUTED side (overwritten by
    # newdata). A forgotten computed column shows up here.
    if new_columns:
        print(f'new computed columns found, added at the start: {new_columns}')

    if user_fields_inferred:
        print('user_given_fields not passed - inferred as "csv columns not in newdata":')
        print(f'  USER (kept as typed, {len(user_given_fields)}): {user_given_fields}')
        computed_in_csv = computed_in_csv + new_columns
        print(f'  COMPUTED (in csv AND newdata, overwritten, {len(computed_in_csv)}): {computed_in_csv}')

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
    annote_data = spark.read.csv(refreshpath, header=True).dropDuplicates()
    return dr.csv_date_roullete_parse(annote_data, datecol)


def refresh_with_annotation_gate(newdata, refreshpath, prikey, user_given_fields, datecol='date', join='full'):
    """
    refresh_rows_user_actions + a pause for the human, in one call.

    pass 1  refresh and WRITE the csv back: rows the csv has never seen are
            appended with their user fields blank.
    gate    whenever someone is at a terminal (new rows or not - you may want to
            edit existing annotations): open the csv, wait for Enter, re-read it.
            repeats while any NEW row is still completely blank; 'c' continues
            without re-reading.
    pass 2  refresh from the re-read csv WITHOUT rewriting it: Excel may still
            have it open, and pass 1 already made the timestamped backup.
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
        newdata=newdata, annote_data=load_annotations(spark, refreshpath, datecol),
        prikey=prikey, user_given_fields=user_given_fields,
        join=join,
        computed_columns_sel='annote_data')  # the gate keeps its old behaviour: the csv decides the columns
    # refresh_rows_user_actions no longer writes internally (decoupled from any
    # file 2026-09-29) - this is pass 1's write, done explicitly, same as it
    # always effectively was.
    rw.write_csv_safe(refreshed, refreshpath, backup=True)

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
            newdata=newdata, annote_data=load_annotations(spark, refreshpath, datecol),
            prikey=prikey, user_given_fields=user_given_fields,
            join=join,
            computed_columns_sel='annote_data')
        # pass 2: no write - Excel may still have refreshpath open, and pass 1
        # already made the timestamped backup.

        still_blank = refreshed.join(new_keys, on=prikey, how='inner').filter(all_blank).count()
        if still_blank == 0:
            print(f'[annotate] re-read your edits ({n_new} new row(s), none left blank) - continuing')
            return refreshed
        print(f'[annotate] {still_blank} of {n_new} new row(s) still completely blank')

############


