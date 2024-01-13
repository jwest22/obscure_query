import duckdb

db_file_path = 'demo_data.duckdb'

conn_query = duckdb.connect(db_file_path)
print(conn_query)
df = conn_query.execute("select * from information_schema.tables").fetchdf()
print(df)
