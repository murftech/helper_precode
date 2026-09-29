import inspect
import os

from precode.pre_dimchecks import tic, toc, persist

os.environ

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col


public_hive = '/Users/murftech/Root/hive_qa/'

def nullvalidate_stringread(df):
    def nanshow(data, target, collector):
        print('running for:')
        print(target)
        attacher = data.withColumn('is_nan', when(col(target)=='NaN', lit('is_nan')))
        victim = attacher.filter(col("is_nan")=='is_nan')
        if (is_empty(victim)):
            print('column has no nan')
        if not is_empty(victim):
            dc.sortcount(attacher, 'is_nan')
            print('column has nan')
            print('results for:')
            print(target)
            collector = collector.withColumn(target, when(col(target)=='NaN', None)
                             .otherwise(col(target)))
            print('reassigned in dataset')
        return collector

    collector = df

    for colname in df.columns:
        collector = nanshow(df, colname, collector)

    return collector



def stringread_csv(filepath):
    bb = pd.read_csv(filepath)
    bbnames = bb.columns
    schema = StructType([StructField(col, StringType(), True) for col in bbnames])
    bb = spark.createDataFrame(bb, schema)
    bb.show()
    return bb
    

    

def spark_write_csv(df, filepath):
    import sys
    from packaging.version import Version as Version

    # Patch distutils.version.LooseVersion for PySpark compatibility
    sys.modules['distutils.version'] = __import__('types').SimpleNamespace(
        LooseVersion=Version
    )

    pd_df = df.toPandas()
    pd_df.to_csv(filepath, index=False)


# def write_csv(sparkdf, csvpath):
#     tic()
#     cdsw_csvpath = '/home/cdsw/' + csvpath 
#     destination_folder = os.path.dirname(cdsw_csvpath)

#     if not os.path.exists(destination_folder):
#         print('directory doesnt exist, will be created.')
#         os.makedirs(destination_folder)
#     sparkdf.printSchema()
#     # sparkdf = sparkdf.drop('businessdate', 'monthdate')
#     # The date columns got problem sia

#     schema = sparkdf.schema
#     date_columns = [field.name for field in schema.fields if isinstance(field.dataType, (DateType, TimestampType))]

#     for column in date_columns:
#         sparkdf = sparkdf.withColumn(column, col(column).cast("string"))

#     sparkdf.toPandas().to_csv(cdsw_csvpath ,index=False, na_rep='')
#     print('file written to: ' + cdsw_csvpath)
#     toc()
    

#### parquet writing ####    
def write_partition(dataframe, partition_list, destination_leaf, bucket = 'private'):  
    
#     alert('writing partition start')
    # print('validation checks')
    # if (rpart(hive_folder_path).columns != mtbgc_churn_schema.columns):
    #     raise Exception ('targetcols list unmatched! do not push! check first')
#     if (dataframe.count()) == 0:
#         raise Exception("empty dataset returned, no point pushing, and definitely there's error in any pipe. Debug!")

    if bucket == 'public':
        absolutepath = public_hive + destination_leaf
        print('will write to:')
        print(absolutepath)
        print('writing partition started')
        tic('io')
        dataframe.write.parquet(absolutepath, mode='overwrite', partitionBy = partition_list)
        print('writing partition done')
    #     alert('writing partition end')
        toc('io')

    if bucket == 'private':
        print('writing partition started')
        tic('io')
        dataframe.write.parquet(destination_leaf, mode='overwrite', partitionBy = partition_list)
        print('writing partition done')
    #     alert('writing partition end')
        toc('io')



def rpart(destination_leaf, arg_country=None, datecol=None, startDate=None, endDate=None, bucket = 'private'):
    if bucket == 'public':
        absolutepath = public_hive + destination_leaf
        print(absolutepath)
        tic('rpart run')
        load_parquet = spark.read.parquet(absolutepath)
        if arg_country is not None:
            print('applied country filter: ' + str(arg_country))
            load_parquet = load_parquet.filter(col('country').isin(arg_country))
        if datecol is not None:
            print('applied daterange filter: ' + startDate + ' ~ ' + endDate)
            load_parquet = dfilter(load_parquet, datecol, startDate, endDate)

        print('schema of loaded table:')
        load_parquet.printSchema()
        toc('rpart run')
        return load_parquet
    
    if bucket == 'private':
        tic('rpart run')
        load_parquet = spark.read.parquet(destination_leaf)
        if arg_country is not None:
            print('applied country filter: ' + str(arg_country))
            load_parquet = load_parquet.filter(col('country').isin(arg_country))
        if datecol is not None:
            print('applied daterange filter: ' + startDate + ' ~ ' + endDate)
            load_parquet = dfilter(load_parquet, datecol, startDate, endDate)

        print('schema of loaded table:')
        load_parquet.printSchema()
        toc('rpart run')
        return load_parquet


import os, re


def hive_list_partition(hive_path):
    # Get one leaf path and extract the key names
    for root, dirs, files in os.walk(hive_path):
        if any(f.endswith('.parquet') for f in files):
            rel = root.replace(hive_path + '/', '')
            partition_cols = re.findall(r'(\w+)=', rel)
            print(partition_cols)  # ['currency_wallet', 'statement_monthdate']
            break  # one path is enough — all partitions follow the same structure
        