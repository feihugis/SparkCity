import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

import pandas as pd

from analysis.csv_util import load_data

import numpy as np
from sklearn.linear_model import Ridge

import statsmodels.api as sm
from scipy.stats import pearsonr
from analysis.basic_statistics import describe_data, correlation_test
from sklearn.model_selection import train_test_split as sklearn_train_test_split

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

CSV_COLUMNS = "osm_id	code	fclass	name	lst	ndvi	ndwi	ndbi	ndii	mndwi	ndisi	CP	MPS	MSI	MNND	PCI	TP	RP".split("\t")
FEATURE_COLUMNS = "ndvi	ndwi	ndbi	ndii	mndwi	ndisi	CP	MPS	MSI	MNND	PCI	TP	RP".split("\t")
LABEL_COLUMN = [LST]
csvfile = "data/result/va/landuse/join_features.csv"


def normalize(df):
    return (df - df.min()) / (df.max() - df.min())


def standardize(df):
    return (df - df.mean()) / df.std()


def train_test_split(df, x_cols, y_col, test_percent, isStandardize=False, isNormalize=False):
    # print(df.head(5))
    df_X = df[x_cols]
    df_y = df[y_col]

    X_train, X_test, y_train, y_test = sklearn_train_test_split(df_X,
                                                                df_y,
                                                                test_size=test_percent,
                                                                random_state=0)

    if isStandardize:
        X_train = standardize(X_train)
        X_test = standardize(X_test)
        return X_train, y_train, X_test, y_test

    if isNormalize:
        X_train = normalize(X_train)
        X_test= normalize(X_test)
        return X_train, y_train, X_test, y_test

    return X_train, y_train, X_test, y_test


def lasso_regression(X_train, y_train, X_test, y_test, normalize=False):
    print("-------------------------- Lasso Regression")
    clf = linear_model.Lasso(alpha=0.01, max_iter=5000)
    clf.fit(X_train, y_train)

    # Make predictions using the testing set
    y_pred = clf.predict(X_test)

    # The intercept
    print("Intercept: %.4f" % clf.intercept_)
    # The mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_test, y_pred))
    # Explained variance score: 1 is perfect prediction
    print('Coefficient of determination(R^2): %.2f' % r2_score(y_test, y_pred))
    # The coefficients
    cols = X_train.columns.tolist()
    coef = clf.coef_.tolist()
    coef = list(zip(cols, coef))
    df_coef = pd.DataFrame.from_records(coef)
    print('Coefficients: \n', df_coef)

    return clf


def ridge_regression(X_train, y_train, X_test, y_test, normalize=False):
    print("-------------------------- Ridge Regression")
    clf = Ridge(alpha=1.50, max_iter=5000)
    clf.fit(X_train, y_train)

    # Make predictions using the testing set
    y_pred = clf.predict(X_test)

    # The intercept
    print("Intercept: %.4f" % clf.intercept_)
    # The mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_test, y_pred))
    # Explained variance score: 1 is perfect prediction
    print('Coefficient of determination(R^2): %.2f' % r2_score(y_test, y_pred))
    # The coefficients
    cols = X_train.columns.tolist()
    coef = clf.coef_.tolist()[0]
    coef = list(zip(cols, coef))
    df_coef = pd.DataFrame.from_records(coef)
    print('Coefficients: \n', df_coef)

    return clf


def linear_regression(X_train, y_train, X_test, y_test, normalize=False):
    print("-------------------------- Linear Regression")
    # Create linear regression object
    regr = linear_model.LinearRegression(normalize=normalize)

    # Train the model using the training sets
    regr.fit(X_train, y_train)

    # Make predictions using the testing set
    y_pred = regr.predict(X_test)

    # The intercept
    print("Intercept: %.4f" % regr.intercept_)
    # The mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_test, y_pred))
    # Explained variance score: 1 is perfect prediction
    print('Coefficient of determination(R^2): %.2f' % r2_score(y_test, y_pred))
    # The coefficients
    cols = X_train.columns.tolist()
    coef = regr.coef_.tolist()[0]
    coef = list(zip(cols, coef))
    df_coef = pd.DataFrame.from_records(coef)
    print('Coefficients: \n', df_coef)

    return regr


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
    df = load_data(csvfile, hasheader=True)
    df = df[(df["CP"] != 0) & (df["TP"] != 0)]
    print(len(df))

    # describe_data(df[CSV_COLUMNS], CSV_COLUMNS)

    result = correlation_test(df[CSV_COLUMNS])
    print(result)

    test_percent = 0.3
    X_train, y_train, X_test, y_test = train_test_split(df,
                                                        FEATURE_COLUMNS,
                                                        LABEL_COLUMN,
                                                        test_percent,
                                                        isNormalize=False,
                                                        isStandardize=True)

    linear_regression(X_train, y_train, X_test, y_test, normalize=False)
    ridge_regression(X_train, y_train, X_test, y_test, normalize=False)
    lasso_regression(X_train, y_train, X_test, y_test, normalize=False)

    result = stepwise_selection(X_train, y_train,
                       initial_list=[],
                       threshold_in=0.01,
                       threshold_out=0.05,
                       verbose=True)

    print('resulting features:')
    print(result)


if __name__ == '__main__':
    main()
