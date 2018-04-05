import pandas as pd


def load_data(file_path, col_names=[], hasheader=False):
    if hasheader:
        x = pd.read_csv(file_path, header=0)
    else:
        x = pd.read_csv(file_path, names=col_names)

    return x


def join_tables(df_left, df_right, how='left', on=["osm_id"]):
    return pd.merge(df_left, df_right, how=how, on=on)


def join_block_table(statename="va"):

    block_table_base = f"data/{statename}/result/{statename}_cb.csv"
    block_table_base_col = "STATEFP,COUNTYFP,TRACTCE,BLKGRPCE,AFFGEOID,GEOID,NAME,LSAD,ALAND,AWATER," \
                           "lst,ndvi,ndwi,ndbi,ndii,mndwi,ndisi".split(",")

    block_table = f"data/{statename}/lst/{statename}_lst_block.csv"
    block_table_columns = "AFFGEOID,area".split(",")

    buildings_table = f"data/{statename}/result/{statename}_buildings.csv"
    buildings_table_columns = "AFFGEOID,CP,MPS,MSI,MNND,PCI,FN".split(",")

    parkings_table = f"data/{statename}/result/{statename}_parkings.csv"
    parkings_table_columns = "AFFGEOID,TP".split(",")

    roads_table = f"data/{statename}/result/{statename}_roads.csv"
    roads_table_columns = "AFFGEOID,RP".split(",")

    water_table = f"data/{statename}/result/{statename}_water.csv"
    water_table_columns = "AFFGEOID,CP".split(",")

    race_table = f"data/{statename}/social/{statename}_race.csv"
    race_table_columns = "GEOID,B02001e1".split(",")

    income_table = f"data/{statename}/social/{statename}_income.csv"
    income_table_columns = "GEOID,B19001e1".split(",")


    df_block_base = load_data(block_table_base, hasheader=True)[block_table_base_col]
    df_block = load_data(block_table, hasheader=True)[block_table_columns]
    df_buildings = load_data(buildings_table, hasheader=True)[buildings_table_columns]
    df_parkings = load_data(parkings_table, hasheader=True)[parkings_table_columns]
    df_roads = load_data(roads_table, hasheader=True)[roads_table_columns]
    df_water = load_data(water_table, hasheader=True)[water_table_columns]

    df_race = load_data(race_table, hasheader=True)[race_table_columns]
    df_race.columns = ['AFFGEOID', 'population']
    df_race['AFFGEOID'] = df_race['AFFGEOID'].apply(lambda id: id.replace("15000", "1500000"))

    df_income = load_data(income_table, hasheader=True)[income_table_columns]
    df_income.columns = ['AFFGEOID', 'income']
    df_income['AFFGEOID'] = df_income['AFFGEOID'].apply(lambda id: id.replace("15000", "1500000"))

    # B19001e1 : house hold income
    # B02001e1: total population


    for df_right in [df_block, df_buildings, df_parkings, df_roads, df_water, df_race, df_income]:
        df_block_base = join_tables(df_block_base, df_right, how='left', on=["AFFGEOID"])

    df_block_base["population"] = df_block_base["population"] / df_block_base["area"]

    df_block_base.fillna(0).to_csv(f"data/{statename}/result/join_feature.csv", index=False)


def join_landuse_table(args=None):
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
    # main()
    join_block_table(statename="md")
    join_block_table(statename="va")
    join_block_table(statename="dc")
