

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

def validify_df_colnames(df):
    new_columns = [col(col_name).alias(col_name.strip().lower().replace(' ', '_')) for col_name in df.columns]
    return df.select(*new_columns)


from precode.pre_dimchecks import tic, toc, persist



count_if = lambda cond: sum(when(cond, 1).otherwise(0))

# testers
# report_table = gc_funnel_monthly_gc
# skus_to_sort = ['country', 'gc']
# sortby_metric = 'nb_triggers'

          
def recode_dfcol(df, colname, condition, litvalue):
    df = (df.withColumn(colname, when(condition, lit(litvalue))
                        .otherwise(col(colname)))
                                  )
    return df


def sorter_compile(report_df, skus_to_sort, metric):
    sku_sorter = (report_df
                       .groupBy(skus_to_sort)
                       .agg(
                           round(mean(metric),0).alias('sorter_' + metric)
                       ))

    nb_total = sku_sorter.agg(sum('sorter_' + metric)).collect()[0][0]

    windowSpec = Window.partitionBy().orderBy(desc('sorter_' + metric))

    sku_sorter_topcum = (sku_sorter
                         .withColumn('pcnt_' + metric, col('sorter_' + metric)/nb_total)
                         .withColumn('top_' + metric, round(sum('pcnt_' + metric).over(windowSpec),3))
                         .withColumn('pcnt_' + metric, round('pcnt_' + metric, 3))
                         .orderBy(desc('pcnt_' + metric))
                        )
    persist(sku_sorter_topcum)
    sku_sorter_topcum.show(1000)
    
    return sku_sorter_topcum



def sorter_things2(report_table, skus_to_sort, sortby_metrics):
    global sku_sorter
    print('sortby_value means: value_to_sort_via_average_across_rows')
    print('sku_sorter available in global')
    sku_sorter=(report_table.groupBy(skus_to_sort)
               .agg(mean(sortby_metrics[0]).alias('sorter1'),
                    mean(sortby_metrics[1]).alias('sorter2')
                   )
               .orderBy(desc('sorter1'), desc('sorter2'))
              )
    if 'sorter1' in report_table.columns:
        report_table = report_table.drop('sorter1', 'sorter2')
        
    report_table_sorter = report_table.join(sku_sorter, on=skus_to_sort, how='left')
    report_table_sorter = report_table_sorter.orderBy(desc('sorter1'), desc('sorter2'))

    return report_table_sorter

def sorter_things(report_table, skus_to_sort, sortby_metrics):
    global sku_sorter
    print('sortby_value means: value_to_sort_via_average_across_rows')
    print('sku_sorter available in global')
    
    sku_sorter=(report_table.groupBy(skus_to_sort)
               .agg(mean(sortby_metrics).alias('sorter'))
               .orderBy(desc('sorter'))
              )
    if 'sorter' in report_table.columns:
        report_table = report_table.drop('sorter')
        
    report_table_sorter = report_table.join(sku_sorter, on=skus_to_sort, how='left')
    report_table_sorter = report_table_sorter.orderBy(desc('sorter'))

    return report_table_sorter

def replace_bvalues_to_a(df, bvalue, avalue):
    df = df.withColumn(avalue, 
                       when(col(bvalue).isNotNull(), col(bvalue))
                       .otherwise(col(avalue)))
    return df


def rearrange_to_front(df, selected_columns):
    selected_columns
    # Reorder the columns to push selected columns to the front
    rearranged_df = df.select(selected_columns + [col for col in df.columns if col not in selected_columns])
    return rearranged_df


def rearrange_to_back(df, selected_columns):
    selected_columns
    # Reorder the columns to push selected columns to the front
    rearranged_df = df.select([col for col in df.columns if col not in selected_columns] + selected_columns)
    return rearranged_df
def rearrange_columns(df, move_cols, anchor_col):
    # Get the list of columns in the DataFrame
    
    if (type(move_cols)==str):
        move_cols=[move_cols]
        
    columns = df.columns
#     print('a')
    # Ensure anchor_col is present in the DataFrame
    if anchor_col not in columns:
        raise ValueError("anchor_col not found in DataFrame")
    
    # Remove move_cols from the list of columns
    for move_col in move_cols:
        if move_col in columns:
            columns.remove(move_col)
    
    # Get the index of the anchor_col
    anchor_index = columns.index(anchor_col)
#     print('a')
    # Insert move_cols to the right of anchor_col
    for move_col in reversed(move_cols):
        columns.insert(anchor_index + 1, move_col)
#     print('a')
#     print(columns)
    # Rearrange the columns in the DataFrame
    df = df.select(columns)
    
    return df



def suffixed_join(df1, df2, on, how, suffix=['a', 'b']):
    outsidekeys_a = set(df1.columns).difference(on)
    outsidekeys_b = set(df2.columns).difference(on)

    df1_suf = df1
    for col_name in outsidekeys_a:
        df1_suf = df1_suf.withColumnRenamed(col_name, f'{col_name}_{suffix[0]}')

    # df1_suf.show()
    df2_suf = df2
    for col_name in outsidekeys_b:
        df2_suf = df2_suf.withColumnRenamed(col_name, f'{col_name}_{suffix[1]}')

    df_returned = df1_suf.join(df2_suf, on=on, how=how)
    return df_returned


        
def stack_dflist(dflist):
    n = len(dflist)
    print(n)
    master = dflist[0]
    # tester
    # i=1
    for i in range(1,n):
        print(i)
        master = master.unionByName(dflist[i])
    return master
        
        
def generalunion(df1, df2):
    for column in [column for column in df2.columns
               if column not in df1.columns]:
        df1 = df1.withColumn(column, lit(None))

    for column in [column for column in df1.columns
               if column not in df2.columns]:
        df2 = df2.withColumn(column, lit(None))

    uniondata = df1.unionByName(df2)

    return uniondata

    
class uniondf:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def intunion(self, dataframe2):
        set_common = set(self.dataframe.columns).intersection(set(dataframe2.columns))
        list_common = list(set_common)
        unioneddf = self.dataframe.select(list_common).union(dataframe2.select(list_common))
        self.dataframe = unioneddf
        return self

    


# mtbgc_lastflow = grouped_top_n(mtbgc_scoped, grouping_tuple='conversation_id', ranking_metric='last_step_timestamp', top_n=1)
# mtbgc_lastflow.show()

# sub can put this into grouped top n function

# df = mtbgc_scoped
# grouptile = 'conversation_id'
# ranking_metric = desc('last_step_timestamp')
# top_n = 1

def window_top_n(df, partition, orderBy, top_n):
    tic('frun')
    windowspec = Window.partitionBy(partition).orderBy(orderBy)
    # past error
#     df = df.withColumn('top_n', row_number().over(windowspec)==top_n)
    df = df.withColumn('top_n', row_number().over(windowspec)<=top_n)
#     df.cache()
    print('removal percentage: read false - negate: turned off the sortcount')
#     sortcount(df, 'top_n')
    df_top_n = df.filter(col('top_n')).drop('top_n')
    # unicity_advanced(df_top_n, grouptile)
    toc('frun')
    return df_top_n


def grouped_top_n(data, grouping_tuple, ranking_metric, top_n):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, row_number
    windowDept = Window.partitionBy(grouping_tuple).orderBy(col(ranking_metric).desc())

    grouped_top_n = (
        data
        .withColumn("rank",row_number().over(windowDept))
        .filter(col("rank") <= top_n)
    )
    return grouped_top_n
