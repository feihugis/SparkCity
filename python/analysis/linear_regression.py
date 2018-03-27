import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

import pandas as pd

from analysis.csv_util import load_data

import numpy as np
import statsmodels.api as sm
from scipy.stats import pearsonr


POLYGON_ID = "id"
LST = "lst"
NDVI = "ndvi"
NDWI = "ndwi"
NDBI = "ndbi"
NDII = "ndii"
MNDWI = "mndwi"
NDISI = "ndisi"


CP = "CP"
MNND = "MNND"
PCI = "PCI"

CSV_COLUMNS = [LST, NDVI, NDWI, NDBI, NDII, MNDWI, NDISI]
FEATURE_COLUMNS = [NDVI, NDWI, NDBI, NDII, MNDWI, NDISI]

LABEL_COLUMN = [LST]


osm_layer = "pois"
csvfile = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_{}/lst_va_{}.csv".format(osm_layer,
                                                                                        osm_layer)

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


def normalize(df):
    return (df - df.min()) / (df.max() - df.min())


def standardize(df):
    return (df - df.mean()) / df.std()


def gen_model_input(df, x_cols, y_col, test_percent, isStandardize=False, isNormalize=False):
    # print(df.head(5))
    df_X = df[x_cols]
    df_y = df[y_col]

    test_size = int(len(df) * test_percent)

    X_train = df_X[: -1 * test_size]
    y_train = df_y[:-1 * test_size]

    X_test = df_X[-1 * test_size:]
    y_test = df_y[-1 * test_size:]

    if isStandardize:
        X_train = standardize(X_train)
        X_test = standardize(X_test)
        return X_train, y_train, X_test, y_test

    if isNormalize:
        X_train = normalize(X_train)
        X_test= normalize(X_test)
        return X_train, y_train, X_test, y_test

    return X_train, y_train, X_test, y_test


def linear_regression(X_train, y_train, X_test, y_test, normalize=False):
    # Create linear regression object
    regr = linear_model.LinearRegression(normalize=normalize)

    # Train the model using the training sets
    regr.fit(X_train, y_train)

    # Make predictions using the testing set
    y_pred = regr.predict(X_test)

    # The coefficients
    print('Coefficients: \n', regr.coef_)
    # The intercept
    print("Intercept: %.4f" % regr.intercept_)
    # The mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_test, y_pred))
    # Explained variance score: 1 is perfect prediction
    print('Coefficient of determination(R^2): %.2f' % r2_score(y_test, y_pred))

    # Plot outputs
    # plt.scatter(X_test, y_test, color='black')
    # plt.plot(X_test, y_pred, color='blue', linewidth=3)
    #
    # plt.xticks(())
    # plt.yticks(())
    #
    # plt.show()

def stepwise_selection(X, y,
                       initial_list=[],
                       threshold_in=0.01,
                       threshold_out = 0.05,
                       verbose=True):
    """ Perform a forward-backward feature selection
    based on p-value from statsmodels.api.OLS
    Arguments:
        X - pandas.DataFrame with candidate features
        y - list-like with the target
        initial_list - list of features to start with (column names of X)
        threshold_in - include a feature if its p-value < threshold_in
        threshold_out - exclude a feature if its p-value > threshold_out
        verbose - whether to print the sequence of inclusions and exclusions
    Returns: list of selected features
    Always set threshold_in < threshold_out to avoid infinite looping.
    See https://en.wikipedia.org/wiki/Stepwise_regression for the details
    """
    included = list(initial_list)
    while True:
        changed=False
        # forward step
        excluded = list(set(X.columns)-set(included))
        new_pval = pd.Series(index=excluded)
        for new_column in excluded:
            model = sm.OLS(y, sm.add_constant(pd.DataFrame(X[included+[new_column]]))).fit()
            new_pval[new_column] = model.pvalues[new_column]
        best_pval = new_pval.min()
        if best_pval < threshold_in:
            best_feature = new_pval.argmin()
            included.append(best_feature)
            changed=True
            if verbose:
                print('Add  {:30} with p-value {:.6}'.format(best_feature, best_pval))

        # backward step
        model = sm.OLS(y, sm.add_constant(pd.DataFrame(X[included]))).fit()
        # use all coefs except intercept
        pvalues = model.pvalues.iloc[1:]
        worst_pval = pvalues.max() # null if pvalues is empty
        if worst_pval > threshold_out:
            changed=True
            worst_feature = pvalues.argmax()
            included.remove(worst_feature)
            if verbose:
                print('Drop {:30} with p-value {:.6}'.format(worst_feature, worst_pval))
        if not changed:
            break
    return included


def main(args=None):
    df = load_data(csvfile, hasheader=True) #.sample(8000)

    # describe_data(df[CSV_COLUMNS], CSV_COLUMNS)

    result = correlation_test(df[CSV_COLUMNS])
    print(result)

    test_percent = 0.2
    X_train, y_train, X_test, y_test = gen_model_input(df,
                                                       FEATURE_COLUMNS,
                                                       LABEL_COLUMN,
                                                       test_percent,
                                                       isNormalize=False,
                                                       isStandardize=False)

    #y_train = np.log(y_train)
    #y_test = np.log(y_test)



    linear_regression(X_train, y_train, X_test, y_test, normalize=False)

    result = stepwise_selection(X_train, y_train,
                       initial_list=[],
                       threshold_in=0.01,
                       threshold_out = 0.05,
                       verbose=True)

    print('resulting features:')
    print(result)


if __name__ == '__main__':
    main()
