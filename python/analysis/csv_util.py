import pandas as pd


def load_data(file_path, col_names, hasheader=False):
    if hasheader:
        x = pd.read_csv(file_path, names=col_names, header=0)
    else:
        x = pd.read_csv(file_path, names=col_names)

    return x
