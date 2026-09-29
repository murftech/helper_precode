import time
import polars as pl


def persist(df):
    pass  # Polars is eager by default


def tic(name='mastertic'):
    globals()[name] = time.time()
    if name == 'mastertic':
        print('mastertic timer has been started')


def toc(name='mastertic'):
    now = time.time()
    print(str(name) + ' took (seconds):')
    print(now - globals()[name])


def noexist(varname):
    return varname not in globals()


def exist(varname):
    return varname in globals()


def showcol(df, column):
    print(df.filter(pl.col(column).is_not_null()).select(column))


def is_empty(df):
    return df.height == 0


def emptyerror(df):
    if is_empty(df):
        raise Exception('data empty!')


def listvalues(df, colnames, n=100):
    if isinstance(colnames, str):
        colnames = [colnames]
    uniquevals = df.select(colnames).unique().sort(colnames)
    print(uniquevals.head(n))
    return uniquevals


def dfcol_to_list(df, colname):
    return df[colname].to_list()


def matchrowcount(df1, df2):
    tic('frun')
    global mrc
    rowcount1 = df1.height
    rowcount2 = df2.height
    if rowcount1 == rowcount2:
        print('stayed same. pass.')
        mrc = 0
    else:
        print('rows different! pcnt difference shown')
        pcnt_delta = rowcount2 / rowcount1
        print(pcnt_delta)
        mrc = 1
        print('global indicator mrc==1 available for raising exception')
    toc('frun')


def dfratio(df_before, df_after):
    tic('frun')
    global ratio_delta
    before_count = df_before.height
    after_count = df_after.height
    ratio_delta = after_count / before_count
    print(f'before count: {before_count}')
    print(f'after count: {after_count}')
    print(f'ratiodelta: {ratio_delta:.3f}')
    print('ratio_delta returned as global variable for exception raising')
    toc('frun')


def in_a_b(table_a, table_b, variable):
    tic('frun')
    global intset
    if isinstance(variable, str):
        variable = [variable]
    distint_a = table_a.select(variable).unique().with_columns(pl.lit(1).alias('in_a'))
    distint_b = table_b.select(variable).unique().with_columns(pl.lit(1).alias('in_b'))
    intset = distint_a.join(distint_b, on=variable, how='full', coalesce=True)
    print(intset.group_by(['in_a', 'in_b']).len().sort(['in_a', 'in_b']))
    toc('frun')
    print('results table avail in global env by calling intset')


def sortcount(df, coltuple, truncate=True):
    tic('sortcount_run')
    if isinstance(coltuple, str):
        coltuple = [coltuple]
    factor_table = df.group_by(coltuple).len().rename({'len': 'count'}).sort('count', descending=True)
    table_total = factor_table['count'].sum()
    print(table_total)
    factor_table_pcnt = factor_table.with_columns((pl.col('count') / table_total).alias('pcnt'))
    print(factor_table_pcnt.head(40))
    toc('sortcount_run')
    return factor_table_pcnt


def dim(df):
    tic('frun')
    print(df.height, len(df.columns))
    toc('frun')


def datescope_dataset(df, datecol, breakdown='daily', show=False):
    if breakdown == 'daily':
        list_dates = df.select(datecol).unique().sort(datecol, descending=True)
        if show:
            print(list_dates.head(30))
        return list_dates
    if breakdown == 'monthly':
        list_months = (df
                       .with_columns([
                           pl.col(datecol).dt.year().alias('year'),
                           pl.col(datecol).dt.month().alias('month'),
                       ])
                       .select(['year', 'month']).unique()
                       .sort(['year', 'month'], descending=True))
        if show:
            print(list_months)
        return list_months


def monthscopecheck(table, datecol, grouping=[], inner=0):
    from precode.pre_dateranger_pl import make_yearmonth
    tic('monthscopecheck frun')
    global msc_out
    grouptile = grouping + ['year', 'month']
    dates_sel = table.select(grouping + [datecol]).unique()
    months_sel = make_yearmonth(dates_sel, datecol)

    if inner == 1:
        msc_out = (months_sel
                   .group_by(grouptile)
                   .agg(pl.col(datecol).n_unique().alias('nb_dates'))
                   .sort(grouptile))
        print(msc_out)
    if inner == 0:
        msc_out = listvalues(months_sel, grouptile, n=10000)
    return msc_out


def summarydate(table, datecol, choice='all', grouping=[]):
    global dates_summary
    tic('frun')

    if choice == 'min':
        dates_summary = (table.group_by(grouping)
                         .agg(pl.col(datecol).min().alias('maxdate'))
                         .sort(grouping))
    if choice == 'max':
        dates_summary = (table.group_by(grouping)
                         .agg(pl.col(datecol).max().alias('maxdate'))
                         .sort(grouping))
    if choice == 'all':
        dates_summary = (table.group_by(grouping)
                         .agg([
                             pl.col(datecol).max().alias('maxdate'),
                             pl.col(datecol).min().alias('mindate'),
                         ])
                         .sort(grouping))
    print(dates_summary)
    toc('frun')
    print('dates_summary in global for reprinting if need.')


def unicity(dataset, coltuple, showpcnt=True):
    tic('frun')
    print('victims will be returned in global as uvictims')
    global uvictims
    if isinstance(coltuple, str):
        coltuple = [coltuple]
    unitable = (dataset.group_by(coltuple).len()
                .rename({'len': 'nb_rows'})
                .sort('nb_rows', descending=True))
    multiplicity = unitable.filter(pl.col('nb_rows') > 1)

    if multiplicity.height == 0:
        print("good!: multiplicity passed!:")
        uvictims = None
    else:
        print("warning: multiplicity violated!")
        uvictims = dataset.join(multiplicity.select(coltuple), on=coltuple, how='semi').sort(coltuple)
        print('victims are returned in global as uvictims.')
        if showpcnt:
            print('multiplicity pcnt')
            sortcount(unitable, 'nb_rows', truncate=False)
    print('unicity ended')
    toc('frun')


def prikey_forcecheck(df, prikeycols):
    from precode.pre_datashapers_pl import window_top_n
    tic('prikey_forcecheck run')
    print('prikey checks no matter what, and if prikey dont match just take a simple, drop first row ignore.')
    unicity(df, prikeycols)
    if uvictims is None:
        print('returned the exact same df')
        return df
    else:
        print('prikey broken applying brute dedup')
        df_dedup = window_top_n(df, partition=prikeycols, orderBy=['form_id'], top_n=1)
        print('confirm it is deduped:')
        unicity(df_dedup, prikeycols)
        toc('prikey_forcecheck run')
        return df_dedup


def cdt(dataframe, grouping_tuple, colname):
    if isinstance(grouping_tuple, str):
        grouping_tuple = [grouping_tuple]
    groupcount = (dataframe
                  .group_by(grouping_tuple)
                  .agg(pl.col(colname).n_unique().alias('nb_' + colname))
                  .sort(grouping_tuple))
    print(groupcount.schema)
    return groupcount


def valexists(df, var, value):
    filtered = df.filter(pl.col(var) == value)
    existcheck = filtered.height > 0
    if existcheck:
        showcol(filtered, var)
    return existcheck


def nullpcnt(df, var, groupcol=[]):
    global nullindicator
    print('checking that null doesnt exist first')
    has_nulls = df.filter(pl.col(var).is_null()).height > 0
    print('global variable returned as nullindicator=1 if null values exist')
    if not has_nulls:
        nullindicator = 0
        print('no nulls!')
    else:
        nullindicator = 1
        print('global variable already returned, you can break function')
        print('checking nullpcnt for: ' + var)
        pcntnull = sortcount(
            df.with_columns(pl.col(var).is_not_null().alias(var + '_notnull')),
            var + '_notnull'
        )
        return pcntnull


def dval_nullpcnt(dataframe, groupcol=[], return_results=0):
    from precode.pre_datashapers_pl import stack_dflist, rearrange_to_front
    tic('nullelapsed')
    dflist = []
    for var in dataframe.columns:
        pcntnull = nullpcnt(dataframe, var, groupcol)
        if return_results == 1 and pcntnull is not None:
            pcntnull = pcntnull.rename({pcntnull.columns[0]: 'isNotNull'})
            pcntnull = pcntnull.with_columns(pl.lit(var).alias('variable'))
            dflist.append(pcntnull)
        toc('nullelapsed')

    if return_results == 1 and dflist:
        results = stack_dflist(dflist)
        results = rearrange_to_front(results, ['variable'])
        print(results)
        toc('nullelapsed')
        return results


def columnmatcher(df_a, df_b, prikey, targetcolumn):
    from precode.pre_datashapers_pl import suffixed_join
    if isinstance(prikey, str):
        prikey = [prikey]
    df_a_sel = df_a.select(prikey + [targetcolumn])
    df_b_sel = df_b.select(prikey + [targetcolumn])
    comparison = suffixed_join(df_a_sel, df_b_sel, on=prikey, how='full', suffix=['a', 'b'])
    ta = targetcolumn + '_a'
    tb = targetcolumn + '_b'
    comparison = comparison.with_columns([
        (pl.col(ta).is_null() == pl.col(tb).is_null()).alias('null_match'),
        (pl.col(ta) == pl.col(tb)).alias('values_match'),
    ])
    comparison = comparison.with_columns(
        pl.when(~pl.col('null_match')).then(pl.lit('1 empty, 1 nonnull'))
        .when(pl.col('null_match') & pl.col('values_match').is_not_null()).then(pl.lit('both nonnull'))
        .when(pl.col('null_match') & pl.col('values_match').is_null()).then(pl.lit('both empty'))
        .alias('null_match')
    )
    return comparison


def columnmatcher_stats(table_a, table_b, prikey, valcol, showvictims=False):
    comparison = columnmatcher(table_a, table_b, prikey, valcol)
    print('colmnmatcher for valcol:')
    print(valcol)
    results = sortcount(comparison, ['values_match', 'null_match']).rename({'count': 'number_of_rows'})
    results = results.with_columns(pl.lit(valcol).alias('field'))
    if showvictims:
        print('show values mismatch')
        print(comparison.filter(pl.col('values_match') == False))
        print('show nulls mismatch')
        print(comparison.filter(pl.col('null_match') == '1 empty, 1 nonnull'))
    return comparison, results


def matchbyjoinkey(df_a, df_b, joinkey, metric_col, method='value'):
    tic('run matchbyjoinkey')
    if isinstance(joinkey, str):
        joinkey = [joinkey]
    name_a = metric_col + '_a'
    name_b = metric_col + '_b'

    df_a = (df_a.select(joinkey + [metric_col])
            .filter(pl.col(metric_col).is_not_null())
            .filter(pl.col(metric_col) != 0)
            .rename({metric_col: name_a}))
    df_b = (df_b.select(joinkey + [metric_col])
            .filter(pl.col(metric_col).is_not_null())
            .filter(pl.col(metric_col) != 0)
            .rename({metric_col: name_b}))

    if method == 'value':
        comparison = (df_a.join(df_b, on=joinkey, how='full', coalesce=True)
                      .with_columns((pl.col(name_a) == pl.col(name_b)).alias('metric_match')))
        sortcount(comparison, 'metric_match')
        return comparison

    if method == 'venn':
        in_a_b(df_a, df_b, joinkey)
        intvictims = intset.filter(
            ~((pl.col('in_a').is_not_null()) & (pl.col('in_b').is_not_null()))
        ).sort(['in_a', 'in_b'])
        return intvictims
    toc('run matchbyjoinkey')


def showall(dataframe, *modes):
    tbl_rows = None if 'closey' in modes else -1
    tbl_cols = None if 'closex' in modes else -1
    with pl.Config(tbl_rows=tbl_rows, tbl_cols=tbl_cols):
        print(dataframe)