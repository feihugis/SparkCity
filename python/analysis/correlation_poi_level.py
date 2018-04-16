
from analysis.linear_regression import load_data, train_test_split, linear_regression, ridge_regression, lasso_regression, stepwise_selection, randomforest_regression
from analysis.basic_statistics import filter_by_area


def regression_analysis(csv_file, feature_columns, target_column, fclass="", test_percent=0.3):
    df = load_data(csv_file, hasheader=True)
    df = filter_by_area(df)

    if len(fclass) > 0:
        df = df[(df["fclass"] == fclass)]
    print(len(df))

    #describe_data(df[feature_columns + target_column], feature_columns + target_column)

    X_train, y_train, X_test, y_test = train_test_split(df,
                                                        feature_columns,
                                                        target_column,
                                                        test_percent,
                                                        isNormalize=False,
                                                        isStandardize=True)

    linear_regression(X_train, y_train, X_test, y_test, normalize=False)
    lasso_regression(X_train, y_train, X_test, y_test, normalize=False)
    ridge_regression(X_train, y_train, X_test, y_test, normalize=False)
    randomforest_regression(X_train, y_train, X_test, y_test, max_depth=6)

    # result = stepwise_selection(X_train, y_train,
    #                             initial_list=[],
    #                             threshold_in=0.01,
    #                             threshold_out=0.05,
    #                             verbose=True)
    #
    # print('resulting features:')
    # print(result)


def main():
    statename = "md"
    input_dir = "/Users/feihu/Documents/GitHub/SparkCity/data/"
    csv_file = f"{input_dir}/{statename}/lst/{statename}_lst_pois.csv"
    feature_columns = "ndvi,ndwi,ndbi,ndii,ndisi".split(",")
    target_column = ["lst"]
    test_percent = 0.3
    regression_analysis(csv_file, feature_columns, target_column, "", test_percent)


if __name__ == '__main__':
    main()
