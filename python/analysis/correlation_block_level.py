
from analysis.linear_regression import load_data, train_test_split, linear_regression, ridge_regression, lasso_regression, stepwise_selection, randomforest_regression


def regression_analysis(csv_file, feature_columns, target_column, test_percent = 0.3):
    df = load_data(csv_file, hasheader=True)
    #df = df[(df["CP"] != 0) & (df["TP"] != 0)]
    num_raw_df = len(df)
    #df = df[(df["CP"] != 0) | (df["MPS"] != 0) | (df["MSI"] != 0) |	(df["MNND"] != 0) | (df["PCI"] != 0) | (df["FN"] != 0) | (df["TP"] != 0) | (df["RP"] != 0) | (df["WP"] != 0)]
    df = df[df["FN"] > 3]
    num_filtered_df = len(df)

    print("*************************************************** ", num_filtered_df, "/", num_raw_df)

    #describe_data(df[feature_columns + target_column], feature_columns + target_column)

    #result = correlation_test(df[feature_columns + target_column])
    #print(result)

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
    statename = "dc"
    input_dir = "/Users/feihu/Documents/GitHub/SparkCity/data/20171228/"
    csv_file = f"{input_dir}/{statename}/result/join_feature.csv"
    # CP : building percentage
    feature_columns = "ndvi,ndwi,ndbi,ndii,mndwi,ndisi,CP,WP,MPS,MSI,MNND,PCI,FN,TP,RP,population,income".split(",")
    feature_columns = "CP,RP,TP,WP,MPS,MSI,MNND,PCI,population,income".split(",")
    target_column = ["lst"]
    test_percent = 0.2
    regression_analysis(csv_file, feature_columns, target_column, test_percent)


if __name__ == '__main__':
    main()
