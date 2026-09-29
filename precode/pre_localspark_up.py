

    # from pyspark.sql import SparkSession
    # return SparkSession.builder.master("local[*]").getOrCreate()


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def get_spark(app_name="local", master="local[*]", log_level="WARN"):
    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    spark_conf = SparkConf()
    spark_conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark_conf.set("spark.sql.session.timeZone","UTC")
    spark = SparkSession.builder.appName('localspark').config(conf=spark_conf).getOrCreate()

    #######################################

    # from pyspark.sql import *
    # from pyspark.sql.functions import col, lit, when, input_file_name    
    # from pyspark.sql.functions import *

    spark.sparkContext.setLogLevel(log_level)

    # Your own message so you always see something
    print(f"[precode] Spark started: app={app_name}, master={master}, log={log_level}")

    return spark

