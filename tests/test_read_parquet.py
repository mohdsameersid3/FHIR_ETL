import pyarrow.parquet as pq

table = pq.read_table(
    "data/bronze/Patient/2026-07-18/batch_00001.parquet"
)

df = table.to_pandas()

print(df.head())

print(df.columns.tolist())

print(df.shape)