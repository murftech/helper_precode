##############
# pre wrappers for user refresh
#############

def coltype_comparison(df_a, df_b, prikey):
    types_a = dict(df_a.schema)
    types_b = dict(df_b.schema)

    rows = []
    for target_col in prikey:
        type_a = types_a.get(target_col)
        type_b = types_b.get(target_col)
        rows.append({
            'col_name': target_col,
            'df_a_type': str(type_a),
            'df_b_type': str(type_b),
            'match_flag': type_a == type_b,
        })

    summary_df = pl.DataFrame(rows)
    print(summary_df)
    return summary_df


def timestamp_duplicate(filepath):
    from pathlib import Path
    from datetime import datetime
    import shutil
    import re

    ext = re.search(r'\..+$', filepath).group(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_path = filepath + '_' + timestamp + ext
    shutil.copy2(filepath, new_path)
    print("Duplicated file in the same folder to:", new_path)

# testers
# newdata = dit_skincare_sel
# mnl_data = mnl_file
# prikey = ['portion_id']
# user_given_fields = user_cols
# refresh_now='y'
# datecol = 'payment_date'
# orderby = ['cleaned_item']

def refresh_rows_user_actions(newdata, mnl_data, prikey, user_given_fields, refresh_now, datecol, orderby=None, refreshpath=None):

    fixed_col_order = mnl_data.columns
    mnl_data = mnl_data.sort(prikey, descending=True)
    print(mnl_data)

    type(mnl_data)
    print(mnl_data)
    mnl_data_user_only = mnl_data.select(prikey + user_given_fields)
    print(mnl_data_user_only)

    print(newdata.schema)

    print('railguard 1: the join keys needs to match typing')
    typer = coltype_comparison(newdata, mnl_data, prikey)
    print(typer)

    if typer.filter(~pl.col('match_flag')).height > 0:
        raise Exception('given typing is not matching')
    else:
        print('join keys type match passed')

    print('railguard 2: here if the joins are nothing, something is terribly wrong.')
    dc.in_a_b(newdata, mnl_data_user_only, prikey)

    newdata_plus_user_fields = newdata.join(mnl_data_user_only, on=prikey, how='left')

    print('NOTE: if i require new computed columns, please define them in the csv first')
    refreshed_data = newdata_plus_user_fields.select(fixed_col_order)

    if orderby is None:
        refreshed_data = refreshed_data.sort(prikey, descending=True)
    else:
        print('reach this stage?')
        refreshed_data = refreshed_data.sort(orderby, descending=True)

    print(refreshed_data[datecol].max())
    print(refreshed_data)
    dc.dim(refreshed_data)

    if refresh_now == 'y':
        print('replace = yes given, hence update spark_write_csv done at the same time`')
        timestamp_duplicate(refreshpath)
        refreshed_data.write_csv(refreshpath)


    return refreshed_data   

