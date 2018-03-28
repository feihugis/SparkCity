from analysis.csv_util import load_data
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np


def describe_data(df, col_names):

    # density plot
    df.plot(kind='density', subplots=True, layout=(len(col_names), 1), sharex=False, figsize=(10, 10))
    correlations = df.corr()
    plt.show()

    # plot correlation matrix
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)
    cax = ax.matshow(correlations, vmin=-1, vmax=1)
    fig.colorbar(cax)
    ticks = np.arange(0, len(col_names), 1)
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    ax.set_xticklabels(col_names)
    ax.set_yticklabels(col_names)
    plt.show()

    # Scatterplot Matrix
    pd.scatter_matrix(df, figsize=(10, 10))
    plt.show()


def correlation_test(df, method='pearson'):
    return df.corr(method = method)


'''
csvfile = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_pois/lst_va_pois.csv"
schema = ['osm_id', 'code', 'fclass', 'name', 'temp', 'ndvi', 'longitude', 'latitude', 'area']

selected_cols = ['fclass', 'name', 'temp', 'longitude', 'latitude', 'ndvi']

df_raw = load_data(csvfile, schema, hasheader=True)

df_sort = df_raw.sort_values(by=['temp'], ascending=False)



row_num = len(df_raw)

percent = 0.1

df_top = df_sort[selected_cols].head(int(row_num*percent))
df_tail = df_sort[selected_cols].tail(int(row_num*percent))

df_top.sort_values(by=['fclass']).to_csv("data/analysis/top_001.csv")
df_tail.sort_values(by=['fclass']).to_csv("data/analysis/tail_001.csv")

df_top[df_top['fclass'] == 'pitch'].to_csv("data/analysis/top_pitch.csv")
df_tail[df_tail['fclass'] == 'pitch'].to_csv("data/analysis/tail_pitch.csv")

top_count = df_top['fclass'].value_counts()
tail_count = df_tail['fclass'].value_counts()

top_count.to_csv("data/analysis/top_count.csv")
tail_count.to_csv("data/analysis/tail_count.csv")

fig = plt.figure(figsize=(17, 10))

top_count.plot(kind='bar')

'''




