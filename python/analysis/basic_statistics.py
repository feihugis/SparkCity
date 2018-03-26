from analysis.csv_util import load_data
import matplotlib.pyplot as plt

csvfile = "/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_pois/lst_va_pois.csv"
schema = ['osm_id', 'code', 'fclass', 'name', 'temp', 'x', 'y']

df_raw = load_data(csvfile, schema, hasheader=True)

df_sort = df_raw.sort_values(by=['temp'], ascending=False)



row_num = len(df_raw)

percent = 0.1

df_top = df_sort[['fclass', 'name', 'temp', 'x', 'y']].head(int(row_num*percent))
df_tail = df_sort[['fclass', 'name', 'temp', 'x', 'y']].tail(int(row_num*percent))

df_top.to_csv("top_0.01.csv")
df_tail.to_csv("tail_0.01.csv")

df_top[df_top['fclass'] == 'pitch'].to_csv("top_pitch.csv")
df_tail[df_tail['fclass'] == 'pitch'].to_csv("tail_pitch.csv")

top_count = df_top['fclass'].value_counts()
tail_count = df_tail['fclass'].value_counts()

top_count.to_csv("top_count.csv")
tail_count.to_csv("tail_count.csv")

fig = plt.figure(figsize=(17, 10))

top_count.plot(kind='bar')






