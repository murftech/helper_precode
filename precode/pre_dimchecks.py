

import time

from pyspark.sql.functions import *

def persist(df):
    df.cache()

# def tic():
#     global timestart 
#     timestart= time.time()




def columnmatcher(df_a, df_b, prikey, targetcolumn):
    df_a.join(df_b, on=prikey)

    df_a_sel = df_a.select(prikey + [targetcolumn])
    df_b_sel = df_b.select(prikey + [targetcolumn])

    comparison = suffixed_join(df_a_sel, df_b_sel, on=prikey, how='outer', suffix=['a', 'b'])

    comparison = (comparison
                  .withColumn('null_match', col(targetcolumn+'_a').isNull()==col(targetcolumn+'_b').isNull())
                  .withColumn('values_match', col(targetcolumn+'_a')==col(targetcolumn+'_b'))
                 )
    
    comparison = (comparison
                   .withColumn('null_match', 
    when(~col('null_match'), lit('1 empty, 1 nonnull'))
    .when( (col('null_match')) & col('values_match').isNotNull(), lit('both nonnull'))
    .when( (col('null_match')) & col('values_match').isNull(), lit('both empty'))
                              )
                  )

# nullmatch	valuesmatch	nullcorrect
# FALSE	Null	1 empty, 1 nonempty
# TRUE	TRUE	both non empty
# TRUE	FALSE	both non empty
# TRUE	Null	both empty
#     persist(comparison)
#     comparison.show()

    return comparison

def columnmatcher_stats(table_a, table_b, prikey, valcol, showvictims=False):
    comparison = columnmatcher(table_a, table_b, prikey, valcol)
    persist(comparison)
    # monthscopecheck(comparison, 'date')
    # comparison.show()

    print('colmnmatcher for valcol:')
    print(valcol)
    results = sortcount(comparison, ['values_match', 'null_match']).withColumnRenamed('count', 'number_of_rows')
    results = results.withColumn('field', lit(valcol))
    
    if showvictims == True:
        ######### only needed if no match ##########

#         match = comparison.filter(col('match') == True)
#         match.show(100, truncate=False)
#         sortcount(match, 'monthdate')

        print('show values mismatch')
        notmatch = comparison.filter(col('values_match') == False)
        notmatch.show(100, truncate=False)
#         sortcount(notmatch, 'monthdate')
        # SG is great.

        print('show nulls mismatch')
        nullmatch = comparison.filter(col('null_match') == '1 empty, 1 nonnull')
        nullmatch.show(100, truncate=False)
    
    return comparison, results


def showcol(df, column):
    df.filter(col(column).isNotNull()).select(column).show(truncate=False)
    
    
def is_empty(df):
    return len(df.head(1))==0

def emptyerror(df):
    if (is_empty(df)):
        raise Exception('data empty!')
        
# def toc(message='elapsed time'):
#     print(message)
#     now = time.time()
#     print(now - timestart)

# def create_variable(name):
#     globals()[name] = 2
# # Example usage:
# name = "my_variable"
# create_variable(name)
# print(my_variable)  # Access the dynamically created variable
def noexist(varname):
    if varname not in globals():
        return True
    if varname in globals():
        return False

def exist(varname):
    if varname not in globals():
        return False
    if varname in globals():
        return True
    
def tic(name='mastertic'):
    globals()[name] = time.time()
    global timestart 
    if name == 'mastertic':
        print('mastertic timer has been started')

def toc(name='mastertic'):
    now = time.time()
    print(str(name) + ' took (seconds):')
    print(now - globals()[name])


def listvalues(df, colnames, n=100):
    uniquevals = df.select(colnames).distinct().orderBy(colnames)
    uniquevals.show(n, truncate=False)
    return uniquevals
    
def dfcol_to_list(df, colname):
    column_list = df.select(colname).rdd.flatMap(lambda x: x).collect()
    return column_list

def matchrowcount(df1, df2):
    tic('frun')
    global mrc
    
    tic('count1')
    rowcount1 = df1.count()
    toc('frun')
    
    tic('count2')
    rowcount2 = df2.count()
    toc('count2')
    
    if rowcount1 == rowcount2:
        print('stayed same. pass.')
        mrc=0
    if rowcount1 != rowcount2:
        print('rows different! pcnt difference shown')
        pcnt_delta = rowcount2/rowcount1
        print(pcnt_delta)
        mrc=1
        print('global indicator mrc==1 available for raising exception')
    tic('frun')


def dfratio(df_before, df_after):
    tic('frun')
    global ratio_delta
    before_count = df_before.count()
    after_count = df_after.count()
    ratio_delta = after_count/before_count
    print(f'before count: {before_count}')
    print(f'after count: {after_count}')
    print(f'ratiodelta: {(ratio_delta, 3)}')
    print('ratio_delta returned as global variable for exception raising')
    toc('frun')
    
def in_a_b(table_a, table_b, variable):
    tic('frun')
    global intset
    distint_a = table_a.select(variable).distinct().withColumn('in_a', lit(1))
    distint_b = table_b.select(variable).distinct().withColumn('in_b', lit(1))
    intset = distint_a.join(distint_b, on = variable, how='outer')
    intset.cache()
    intset.groupBy('in_a', 'in_b').count().orderBy('in_a', 'in_b').show()
    toc('frun')
    print('results table avail in global env by calling intset')
    

def sortcount(df, coltuple, truncate=True):
    tic('sortcount_run')
    factor_table = df.groupBy(coltuple).count().sort(desc('count'))
    tic('factor count')
    factor_table.cache()
    factor_table.show()
    toc('factor count')
    # 292s
    
    tic('total table')
    table_total = factor_table.agg(sum('count')).first()[0]
    print(table_total)
    toc('total table')
    
    # i think it was df .counting for every factor i had... fuck. so it would never finish.
    factor_table_pcnt = factor_table.withColumn('pcnt', col('count')/table_total)
    factor_table_pcnt.cache()
    factor_table_pcnt.show(40, truncate=truncate)
    toc('sortcount_run')
    return factor_table_pcnt

def sortcount_old(df, coltuple, truncate=True):
    tic('sortcount_run')
    factor_table = df.groupBy(coltuple).count().sort(desc('count'))
#     totals = df.count().withColumnRenamed('count', 'total')
#     factor_table_pcnt = factor_table.crossJoin(totals).withColumn('pcnt', col('count')/df.count())
    factor_table_pcnt = factor_table.withColumn('pcnt', col('count')/df.count())
    factor_table_pcnt.cache()
    factor_table_pcnt.show(40, truncate=truncate)
    toc('sortcount_run')
    return factor_table_pcnt


def dim(self):
    tic('frun')
    print(self.count(), len(self.columns))
    toc('frun')



def datescope_dataset(df, datecol, breakdown = 'daily', show=False):

    if breakdown == 'daily':
        list_dates = df.select(datecol).distinct().orderBy(desc(datecol))
        if show:
            list_dates.cache()
            list_dates.show(30)
        return list_dates
    if breakdown == 'monthly':
        df = (df
             .withColumn('year', year(col(datecol)))
             .withColumn('month', month(col(datecol)))
             )
        list_months = df.select('year', 'month').distinct().orderBy(desc('year'), desc('month'))
        if show:
            list_months.cache()
            list_months.show()
        return list_months
    
    

def monthscopecheck(table, datecol, grouping=[], inner=0):
    tic('monthscopecheck frun')
    global msc_out
    grouptile = grouping + ['year', 'month']
#     dates_sel = table.select(grouping + [datecol]).distinct()
    dates_sel = table.dropDuplicates(grouping + [datecol]).select(grouping + [datecol])
    months_sel = make_yearmonth(dates_sel, datecol)
    
    if inner == 1:
        msc_out = months_sel.groupBy(grouptile).agg(countDistinct(datecol).alias('nb_dates')).orderBy(grouptile)
        persist(msc_out)
        msc_out.show(10000)

    if inner == 0:
        msc_out = listvalues(months_sel, grouptile, n=10000)
    
    return msc_out
    print('results avail in global as msc_out')
    toc('monthscopecheck frun')

    

# def monthlydates(table, datecol, grouping=[]):
#     tic('frun')
#     grouptile = grouping + ['year', 'month']
#     dates_sel = table.select(grouping + [datecol]).distinct()
#     months_sel = make_yearmonth(dates_sel, datecol)
#     months_sel.groupBy(grouptile).agg(countDistinct('datecol'))
#     toc('frun')
    
    

def summarydate(table, datecol, choice='all', grouping=[], ):
    global dates_summary
    tic('frun')
    
    if choice=='min':

        dates_summary = (table
                     .groupBy(grouping)
                     .agg(
                         min(datecol).alias('maxdate'),
                     ).orderBy(grouping)
                        )

    if choice=='max':
        dates_summary = (table
                     .groupBy(grouping)
                     .agg(
                         max(datecol).alias('maxdate')
                     ).orderBy(grouping)
                        )
    
    if choice=='all':
        dates_summary = (table
                     .groupBy(grouping)
                     .agg(
                         max(datecol).alias('maxdate'),
#                          mean(datecol).alias('meandate'),
                         min(datecol).alias('mindate'),
                     ).orderBy(grouping)
                        )    
    dates_summary.cache()
    dates_summary.show(1000, truncate=False)
    toc('frun')
    
    print('dates_summary in global for reprinting if need.')


    

def unicity_old(dataset, coltuple):
    unitable = dataset.groupby(coltuple).count().sort(desc('count'))
    multiplicity = unitable.filter(col('count') > 1)
    # unitable.show()
    score = multiplicity.count()

    if (score == 0):
        print("good!: multiplicity passed!:")

    if (score != 0):
        print("warning: multiplicity violated! Showing rows:")
        multiplicity.show()   
        
def unicity(dataset, coltuple, showpcnt=True):
    tic('frun')
    print('victims will be returned in global as uvictims')
    global uvictims
    unitable = dataset.groupby(coltuple).count().withColumnRenamed('count', 'nb_rows').sort(desc('nb_rows'))
    # in unicity it really is this part that takes a long time.
    persist(unitable)

    multiplicity = unitable.filter(col('nb_rows') > 1)
    
    if (is_empty(multiplicity)==True):
        print("good!: multiplicity passed!:")
        uvictims = None
    else:
        print("warning: multiplicity violated!")
        # multiplicity.show(truncate=False) 
        uvictims = dataset.join(multiplicity, on=coltuple, how='leftsemi').orderBy(coltuple)
    
        print('victims are returned in global as uvictims. can cancel the show pcnt if you want')
        
        if showpcnt == True:
            print('multiplicity pcnt')
            sortcount(unitable, 'nb_rows', truncate=False)
            
    print('unicity ended')
    print(f'results were for {dataset}')
    toc('frun')

# fouind this out on 3 april
# sub figure out how did it return the correct values almost every time.
def unicity_old_and_wrong(dataset, coltuple):
    tic('frun')
    print('victims will be returned in global as uvictims')
    global uvictims
    unitable = dataset.groupby(coltuple).count().withColumnRenamed('count', 'nb_rows').sort(desc('count'))
    multiplicity = unitable.filter(col('count') > 1)
#     persist(unitable)
    persist(multiplicity)
    score = multiplicity.count()
    
    # unitable.show()
    if (score == 0):
        print("good!: multiplicity passed!:")
        uvictims = None
        
    if (score != 0):
        print("warning: multiplicity violated!")
        # multiplicity.show(truncate=False) 
        print('multiplicity pcnt')
#         sortcount(unitable, 'nb_rows')
        uvictims = dataset.join(multiplicity, on=coltuple, how='leftsemi').orderBy(coltuple)
    
        print('victims are returned in global as uvictims')
    print('unicity ended')
    toc('frun')

def prikey_forcecheck(df, prikeycols):
    tic('prikey_forcecheck run')
    print('prikey checks no matter what, and if prikey dont match just take a simple, drop first row ignore.')
    print('unicheck, if passes, function ends')
    unicity(df, prikeycols)
    if uvictims is None:
        print('returned the exact same df')
        return df
    
    if uvictims is not None:
        print('prikey broken applying brute dedup')
        df.dropDubplicates('form_id')
        
        df_dedup = window_top_n(df, partition=prikeycols, orderBy=['form_id'], top_n=1)
#         df_dedup = df.dropDuplicates(prikeycols)
        print('confirm it is deduped:')
        unicity(df_dedup, prikeycols)
        toc('prikey_forcecheck run')
        return df_dedup
    

def cdt(dataframe, grouping_tuple, colname):
    groupcount = dataframe.groupBy(grouping_tuple).agg(countDistinct(colname).alias('nb_'+ colname))
    groupcount = groupcount.orderBy(grouping_tuple)
    groupcount.printSchema()
    return groupcount


########
#### sanity check wrappers
#######


def valexists(df, var, value):
    existcheck = ~is_empty(df.filter(col(var)==value))
    if existcheck:
        showcol(df.filter(col(var)==value), var)
    return existcheck

def nullpcnt(df, var, groupcol = []):
    global nullindicator
    
    print('checking that null doesnt exist first')
    nullcheck = is_empty(df.filter(col(var).isNull()))
    print('global variable returned as nullindicator=1 if null values exist')
    if (nullcheck==True):
        nullindicator = 0
        print('no nulls!')
        nullindicator = 0
    if (nullcheck== False):
        nullindicator = 1
        print('global variable already returned, you can break function')
        print('checking nullpcnt for: ' + var)
        grouptile =  [col(var).isNotNull()] + groupcol
        pcntnull = sortcount(df, grouptile)    
        return pcntnull



def dval_nullpcnt(dataframe, groupcol =[], return_results=0):
    tic('nullelapsed')
    dflist = []

    for var in dataframe.columns:
        pcntnull = nullpcnt(dataframe, var, groupcol)

#         pcntnull = sortcount(dataframe, col(var).isNotNull())

        if return_results==1:
            first_column_name = pcntnull.columns[0]
            pcntnull = pcntnull.withColumnRenamed(first_column_name, 'isNotNull')
            pcntnull = pcntnull.withColumn('variable', lit(var))
            dflist.append(pcntnull)
        toc('nullelapsed')


    if return_results==1:
        results = stack_dflist(dflist)
        results = rearrange_to_front(results, ['variable'])
        results.cache()
        print('returned total results')
        results.show()
        toc('nullelapsed')
        return results

    
              

        

def matchbyjoinkey(df_a, df_b, joinkey, metric_col, method = 'value'):
    tic('run matchbyjoinkey')
    
    name_a = metric_col + '_a'
    name_b = metric_col + '_b'
    
    df_a = (df_a
            .select(joinkey + [metric_col])
            .filter(col(metric_col).isNotNull())
            .filter(col(metric_col)!=0)
            .withColumnRenamed(metric_col,name_a)
           )
    df_b = (df_b
            .select(joinkey + [metric_col])
            .filter(col(metric_col).isNotNull())
            .filter(col(metric_col)!=0)
            .withColumnRenamed(metric_col,name_b)
           )
    
    if method == 'value':
        comparison = (df_a
                  .join(df_b, on=joinkey, how='outer')
                  .withColumn('metric_match', col(name_a)==col(name_b))
                 )
        persist(comparison)
        sortcount(comparison, 'metric_match')
        return comparison
    
    if method == 'venn':
        in_a_b(df_a, df_b, joinkey)
        intvictims = intset.filter(~ ((col('in_a').isNotNull()) & (col('in_b').isNotNull()))).orderBy('in_a', 'in_b')
        return intvictims
    toc('run matchbyjoinkey')
