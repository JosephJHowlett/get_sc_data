import get_sc_data as sc

def rename_cols(df, pac='PUR'):
    cols = df.columns.values
    renamer = {col: col.split('%s_' % pac)[-1].split('_')[0] for col in cols if pac in col}
    return df.rename(columns=renamer)

x = sc.get_df(
    ['XE1T.PUR_DPT288_PMON_A.PI', 'XE1T.PUR_PT287_PPUMP.PI'],
    '2019-10-01 19:00:00',
    '2019-10-02 19:00:00',
    time_interval=60.,
    query_type='lab'
)
x = rename_cols(x)

print(x.head())
