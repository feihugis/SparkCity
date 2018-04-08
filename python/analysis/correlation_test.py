from analysis.csv_util import load_data
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import os
from string import ascii_letters
import seaborn as sns

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})


def correlation_test(df, method='pearson'):
    return df.corr(method=method)


def visualize_corr(corr):
    # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(220, 10, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    # sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
    #             square=True, linewidths=.5, cbar_kws={"shrink": .5})

    sns.set(font_scale=2.4)
    sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool),
                cmap=sns.diverging_palette(220, 10, as_cmap=True),
                cbar_kws={"shrink": .5},
                square=True, ax=ax, linewidths=.5,
                annot_kws={"size": 120})

    plt.yticks(rotation=0, fontsize=18)
    plt.xticks(rotation=270, fontsize=18)

    plt.show()


if __name__ == '__main__':
    csv_file = "/Users/feihu/Documents/GitHub/SparkCity/data/md/result/join_feature.csv"
    df = load_data(csv_file, hasheader=True)
    columns = "lst,CP,MPS,MSI,MNND,PCI,TP,RP,WP,population,income".split(",")
    df = df[columns]
    df.columns = "LST,PBA,MPS,MSI,MNND,PCI,PPA,PRA,PWA,Population,Income".split(",")
    corr = correlation_test(df)
    print(corr)
    visualize_corr(corr)

    plt.show()
