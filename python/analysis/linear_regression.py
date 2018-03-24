import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

import pandas as pd

POLYGON_ID = "id"
TEMPERATURE = "temperature"
CP = "CP"
MNND = "MNND"
PCI = "PCI"

CSV_COLUMNS = [POLYGON_ID, TEMPERATURE, CP, MNND, PCI]
FEATURE_COLUMNS = [CP, MNND, PCI]
LABEL_COLUMN = [TEMPERATURE]

csvfile = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va.csv/lst_va.csv"


def load_data(file_path, col_names):
    X = pd.read_csv(file_path, names=col_names)
    return X


def gen_model_input(df, x_cols, y_col, y_test_size):
    print(df)
    df_X = df[x_cols]
    df_y = df[y_col]

    X_train = df_X[: -1 * y_test_size]
    y_train = df_y[:-1 * y_test_size]

    X_test = df_X[-1 * y_test_size:]
    y_test = df_y[-1 * y_test_size:]

    return X_train, y_train, X_test, y_test


# df = load_data(csvfile, CSV_COLUMNS)
# X_train, y_train, X_test, y_test = gen_model_input(df, FEATURE_COLUMNS, LABEL_COLUMN, 100)


def linear_regression(csvfile, y_test_size):
    df = load_data(csvfile, col_names=CSV_COLUMNS)
    X_train, y_train, X_test, y_test = gen_model_input(df,
                                                       FEATURE_COLUMNS,
                                                       LABEL_COLUMN,
                                                       y_test_size)

    # Create linear regression object
    regr = linear_model.LinearRegression()

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
    print('Variance score: %.2f' % r2_score(y_test, y_pred))

    # Plot outputs
    # plt.scatter(X_test, y_test, color='black')
    # plt.plot(X_test, y_pred, color='blue', linewidth=3)
    #
    # plt.xticks(())
    # plt.yticks(())
    #
    # plt.show()


def main(args=None):
    linear_regression(csvfile, 100)


if __name__ == '__main__':
    main()
