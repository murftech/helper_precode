import polars as pl
from precode.pre_dimchecks_pl import tic, toc, persist


count_if = lambda cond: pl.when(cond).then(1).otherwise(0).sum()


def validify_df_colnames(df):
    return df.rename({c: c.strip().lower().replace(' ', '_') for c in df.columns})


def recode_dfcol(df, colname, condition, litvalue):
    return df.with_columns(
        pl.when(condition).then(pl.lit(litvalue)).otherwise(pl.col(colname)).alias(colname)
    )


def sorter_compile(report_df, skus_to_sort, metric):
    if isinstance(skus_to_sort, str):
        skus_to_sort = [skus_to_sort]
    sku_sorter = (report_df
                  .group_by(skus_to_sort)
                  .agg(pl.col(metric).mean().round(0).alias('sorter_' + metric)))

    nb_total = sku_sorter['sorter_' + metric].sum()

    sku_sorter_topcum = (sku_sorter
                         .sort('sorter_' + metric, descending=True)
                         .with_columns((pl.col('sorter_' + metric) / nb_total).round(3).alias('pcnt_' + metric))
                         .with_columns(pl.col('pcnt_' + metric).cum_sum().round(3).alias('top_' + metric))
                         .sort('pcnt_' + metric, descending=True))
    print(sku_sorter_topcum)
    return sku_sorter_topcum


def sorter_things(report_table, skus_to_sort, sortby_metrics):
    global sku_sorter
    print('sortby_value means: value_to_sort_via_average_across_rows')
    print('sku_sorter available in global')
    if isinstance(skus_to_sort, str):
        skus_to_sort = [skus_to_sort]
    sku_sorter = (report_table.group_by(skus_to_sort)
                  .agg(pl.col(sortby_metrics).mean().alias('sorter'))
                  .sort('sorter', descending=True))
    if 'sorter' in report_table.columns:
        report_table = report_table.drop('sorter')
    report_table_sorter = report_table.join(sku_sorter, on=skus_to_sort, how='left').sort('sorter', descending=True)
    return report_table_sorter


def sorter_things2(report_table, skus_to_sort, sortby_metrics):
    global sku_sorter
    print('sortby_value means: value_to_sort_via_average_across_rows')
    print('sku_sorter available in global')
    if isinstance(skus_to_sort, str):
        skus_to_sort = [skus_to_sort]
    sku_sorter = (report_table.group_by(skus_to_sort)
                  .agg([
                      pl.col(sortby_metrics[0]).mean().alias('sorter1'),
                      pl.col(sortby_metrics[1]).mean().alias('sorter2'),
                  ])
                  .sort(['sorter1', 'sorter2'], descending=True))
    if 'sorter1' in report_table.columns:
        report_table = report_table.drop(['sorter1', 'sorter2'])
    report_table_sorter = (report_table.join(sku_sorter, on=skus_to_sort, how='left')
                           .sort(['sorter1', 'sorter2'], descending=True))
    return report_table_sorter


def replace_bvalues_to_a(df, bvalue, avalue):
    return df.with_columns(
        pl.when(pl.col(bvalue).is_not_null()).then(pl.col(bvalue)).otherwise(pl.col(avalue)).alias(avalue)
    )


def rearrange_to_front(df, selected_columns):
    if isinstance(selected_columns, str):
        selected_columns = [selected_columns]
    remaining = [c for c in df.columns if c not in selected_columns]
    return df.select(selected_columns + remaining)


def rearrange_to_back(df, selected_columns):
    if isinstance(selected_columns, str):
        selected_columns = [selected_columns]
    remaining = [c for c in df.columns if c not in selected_columns]
    return df.select(remaining + selected_columns)


def rearrange_columns(df, move_cols, anchor_col):
    if isinstance(move_cols, str):
        move_cols = [move_cols]
    columns = list(df.columns)
    if anchor_col not in columns:
        raise ValueError("anchor_col not found in DataFrame")
    for move_col in move_cols:
        if move_col in columns:
            columns.remove(move_col)
    anchor_index = columns.index(anchor_col)
    for move_col in reversed(move_cols):
        columns.insert(anchor_index + 1, move_col)
    return df.select(columns)


def suffixed_join(df1, df2, on, how, suffix=['a', 'b']):
    if isinstance(on, str):
        on = [on]
    outsidekeys_a = [c for c in df1.columns if c not in on]
    outsidekeys_b = [c for c in df2.columns if c not in on]
    df1_suf = df1.rename({c: f'{c}_{suffix[0]}' for c in outsidekeys_a})
    df2_suf = df2.rename({c: f'{c}_{suffix[1]}' for c in outsidekeys_b})
    return df1_suf.join(df2_suf, on=on, how=how, coalesce=True)


def stack_dflist(dflist):
    n = len(dflist)
    print(n)
    return pl.concat(dflist, how='diagonal')


def generalunion(df1, df2):
    return pl.concat([df1, df2], how='diagonal')


class uniondf:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def intunion(self, dataframe2):
        common_cols = list(set(self.dataframe.columns).intersection(set(dataframe2.columns)))
        self.dataframe = pl.concat([self.dataframe.select(common_cols), dataframe2.select(common_cols)])
        return self


def window_top_n(df, partition, orderBy, top_n):
    tic('frun')
    if isinstance(partition, str):
        partition = [partition]
    if isinstance(orderBy, str):
        orderBy = [orderBy]
    df_top_n = (df.sort(orderBy)
                .with_columns(pl.int_range(pl.len()).over(partition).alias('top_n'))
                .filter(pl.col('top_n') < top_n)
                .drop('top_n'))
    print('removal percentage: read false - negate: turned off the sortcount')
    toc('frun')
    return df_top_n


def grouped_top_n(data, grouping_tuple, ranking_metric, top_n):
    if isinstance(grouping_tuple, str):
        grouping_tuple = [grouping_tuple]
    return (data.sort(ranking_metric, descending=True)
            .with_columns(pl.int_range(pl.len()).over(grouping_tuple).alias('rank'))
            .filter(pl.col('rank') < top_n)
            .drop('rank'))
