import pandas as pd


def load_data(file_path, col_names=[], hasheader=False):
    if hasheader:
        x = pd.read_csv(file_path, header=0)
    else:
        x = pd.read_csv(file_path, names=col_names)

    return x


def join_tables(df_left, df_right, how='left', on=["osm_id"]):
    return pd.merge(df_left, df_right, how=how, on=on)


def main(args=None):
    landuse = "data/result/va/landuse/landuse.csv"
    landuse_cols = "osm_id	code	fclass	name	lst	ndvi	ndwi	ndbi	ndii	mndwi	" \
                   "ndisi".split("\t")
    buildings = "data/result/va/landuse/buildings.csv"
    buildings_cols = "osm_id	CP	MPS	MSI	MNND	PCI".split("\t")
    parkings = "data/result/va/landuse/parkings.csv"
    parkings_cols = "osm_id	TP".split("\t")
    roads = "data/result/va/landuse/roads.csv"
    roads_cols = "osm_id	RP".split("\t")

    df_landuse = load_data(landuse, hasheader=True)[landuse_cols]
    df_buildings = load_data(buildings, hasheader=True)[buildings_cols]
    df_parkings = load_data(parkings, hasheader=True)[parkings_cols]
    df_roads = load_data(roads, hasheader=True)[roads_cols]

    for df_right in [df_buildings, df_parkings, df_roads]:
        df_landuse = join_tables(df_landuse, df_right, how='left', on=["osm_id"])

    df_landuse.fillna(0).to_csv("data/result/va/landuse/join_features.csv", index=False)



if __name__ == '__main__':
    main()
