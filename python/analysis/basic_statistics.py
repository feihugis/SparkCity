from analysis.csv_util import load_data
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import os

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

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
    return df.corr(method=method)


def top_poi(statename, key='lst'):
    csvfile = f"data/{statename}/lst/{statename}_lst_pois.csv"
    schema = "osm_id,code,fclass,name,lst,ndvi,ndwi,ndbi,ndii,mndwi,ndisi,longitude,latitude,area".split(",")

    df_raw = load_data(csvfile, hasheader=True)
    # va (-77.622, -76.995, 38.658, 39.105)
    df_raw = df_raw[(df_raw['longitude'] > -77.622)
                    & (df_raw['longitude'] < -76.995)
                    & (df_raw['latitude'] > 38.658)
                    & (df_raw['latitude'] < 39.105)]

    df_sort = df_raw.sort_values(by=[key], ascending=False)

    row_num = len(df_raw)

    percent = 0.1

    df_top = df_sort.head(int(row_num*percent))
    df_tail = df_sort.tail(int(row_num*percent))

    dir_path = f"data/{statename}/result/poi/"
    os.makedirs(dir_path, exist_ok=True)

    df_top.sort_values(by=['fclass']).to_csv(f"data/{statename}/result/poi/poi_top_{percent}.csv", header=True)
    df_tail.sort_values(by=['fclass']).to_csv(f"data/{statename}/result/poi/poi_tail_{percent}.csv", header=True)

    df_top[df_top['fclass']=='pitch'].to_csv(f"data/{statename}/result/poi/top_pitch_{percent}.csv", header=True)
    df_tail[df_tail['fclass']=='pitch'].to_csv(f"data/{statename}/result/poi/tail_pitch_{percent}.csv", header=True)

    top_count = df_top['fclass'].value_counts()
    tail_count = df_tail['fclass'].value_counts()

    top_count.to_csv(f"data/{statename}/result/poi/top_count.csv", header=True)
    tail_count.to_csv(f"data/{statename}/result/poi/tail_count.csv", header=True)

    #plt.tight_layout()
    fig = plt.figure(figsize=(12, 9))
    ax = top_count.head(15).plot(kind='bar', title="Top 10%", fontsize=15)
    ax.title.set_fontsize(20)
    fig.show()
    fig.savefig(f"data/{statename}/result/poi/top_count_{percent}.png")

    fig = plt.figure(figsize=(12, 9))
    ax = tail_count.head(15).plot(kind='bar', title="Bottom 10%", fontsize=15)
    ax.title.set_fontsize(20)
    fig.show()
    fig.savefig(f"data/{statename}/result/poi/Bottom_count_{percent}.png")


def descriptive_statistics(statename, layer, label_fontsize=10, title_fontsize=20):
    csvfile = f"data/{statename}/lst/{statename}_lst_{layer.lower()}.csv"
    schema = "fclass,lst".split(",")

    df = load_data(csvfile, hasheader=True)
    print(df.groupby("fclass")["lst"].describe())
    sorted_fclass = df.groupby("fclass")["lst"].median().sort_values(ascending=False).index

    plot = df\
        .pivot_table(index="osm_id", columns="fclass")["lst"][sorted_fclass]\
        .plot(kind='box', figsize=[16,8],
              title=f"Descriptive Statistics of LST by {layer} Type ({statename.upper()})",
              rot=270,
              fontsize=label_fontsize)
    plot.title.set_fontsize(title_fontsize)
    fig = plot.get_figure()

    plt.show()

    dir_path = f"data/{statename}/result/descriptive_statistics/"
    os.makedirs(dir_path, exist_ok=True)
    fig.savefig(f"data/{statename}/result/descriptive_statistics/{statename}_{layer}_ds.png")

    plt.show()


if __name__ == '__main__':
    #top_poi("va")
    descriptive_statistics(statename="va", layer="POIs", label_fontsize=10, title_fontsize=20)
    descriptive_statistics(statename="dc", layer="POIs", label_fontsize=10, title_fontsize=20)
    descriptive_statistics(statename="md", layer="POIs", label_fontsize=10, title_fontsize=20)

    descriptive_statistics(statename="va", layer="Landuse", label_fontsize=15, title_fontsize=20)
    descriptive_statistics(statename="dc", layer="Landuse", label_fontsize=15, title_fontsize=20)
    descriptive_statistics(statename="md", layer="Landuse", label_fontsize=15, title_fontsize=20)







