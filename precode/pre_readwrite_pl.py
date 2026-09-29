import os
import re
import polars as pl
from precode.pre_dimchecks_pl import tic, toc


public_hive = '/Users/murftech/Root/hive_qa/'


def nullvalidate_stringread(df):
    collector = df
    for colname in df.columns:
        print('running for:')
        print(colname)
        try:
            has_nan = (collector[colname].cast(pl.Utf8) == 'NaN').sum() > 0
        except Exception:
            has_nan = False
        if not has_nan:
            print('column has no nan')
        else:
            print('column has nan')
            print('results for:')
            print(colname)
            collector = collector.with_columns(
                pl.when(pl.col(colname).cast(pl.Utf8) == 'NaN')
                .then(None)
                .otherwise(pl.col(colname))
                .alias(colname)
            )
            print('reassigned in dataset')
    return collector


def stringread_csv(filepath):
    df = pl.read_csv(filepath, infer_schema=False)
    print(df)
    return df


def spark_write_csv(df, filepath):
    df.write_csv(filepath)


def write_partition(dataframe, partition_list, destination_leaf, bucket='private'):
    if bucket == 'public':
        absolutepath = public_hive + destination_leaf
        print('will write to:')
        print(absolutepath)
        print('writing partition started')
        tic('io')
        dataframe.write_parquet(absolutepath)
        print('writing partition done')
        toc('io')

    if bucket == 'private':
        print('writing partition started')
        tic('io')
        dataframe.write_parquet(destination_leaf)
        print('writing partition done')
        toc('io')


def rpart(destination_leaf, arg_country=None, datecol=None, startDate=None, endDate=None, bucket='private'):
    if bucket == 'public':
        absolutepath = public_hive + destination_leaf
        print(absolutepath)
        tic('rpart run')
        df = pl.read_parquet(absolutepath)
    else:
        tic('rpart run')
        df = pl.read_parquet(destination_leaf)

    if arg_country is not None:
        print('applied country filter: ' + str(arg_country))
        df = df.filter(pl.col('country').is_in(arg_country))
    if datecol is not None:
        print('applied daterange filter: ' + startDate + ' ~ ' + endDate)
        from precode.pre_dateranger_pl import dfilter
        df = dfilter(df, datecol, startDate, endDate)

    print('schema of loaded table:')
    print(df.schema)
    toc('rpart run')
    return df


def hive_list_partition(hive_path):
    for root, dirs, files in os.walk(hive_path):
        if any(f.endswith('.parquet') for f in files):
            rel = root.replace(hive_path + '/', '')
            partition_cols = re.findall(r'(\w+)=', rel)
            print(partition_cols)
            break
