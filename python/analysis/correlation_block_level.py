
from analysis.linear_regression import load_data, correlation_test, train_test_split, linear_regression, ridge_regression, lasso_regression, stepwise_selection, describe_data, randomforest_regression


def regression_analysis(csv_file, feature_columns, target_column, test_percent = 0.3):
    df = load_data(csv_file, hasheader=True)
    #df = df[(df["CP"] != 0) & (df["TP"] != 0)]
    print(len(df))

    #describe_data(df[feature_columns + target_column], feature_columns + target_column)

    result = correlation_test(df[feature_columns + target_column])
    print(result)

    X_train, y_train, X_test, y_test = train_test_split(df,
                                                        feature_columns,
                                                        target_column,
                                                        test_percent,
                                                        isNormalize=False,
                                                        isStandardize=True)

    linear_regression(X_train, y_train, X_test, y_test, normalize=False)
    lasso_regression(X_train, y_train, X_test, y_test, normalize=False)
    ridge_regression(X_train, y_train, X_test, y_test, normalize=False)
    randomforest_regression(X_train, y_train, X_test, y_test)

    # result = stepwise_selection(X_train, y_train,
    #                             initial_list=[],
    #                             threshold_in=0.01,
    #                             threshold_out=0.05,
    #                             verbose=True)

    #print('resulting features:')
    #print(result)


def main():
    statename = "va"
    csv_file = f"data/{statename}/result/join_feature.csv"
    feature_columns = "ndvi,ndwi,ndbi,ndii,mndwi,ndisi,CP,MPS,MSI,MNND,PCI,FN,TP,RP".split(",")
    feature_columns = "CP,MPS,MSI,MNND,PCI,TP,RP".split(",")
    feature_columns = "CP,MPS,MSI,MNND,PCI,TP,RP".split(",")
    target_column = ["lst"]
    test_percent = 0.3
    regression_analysis(csv_file, feature_columns, target_column, test_percent)


if __name__ == '__main__':
    main()
