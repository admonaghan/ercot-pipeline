import dlt

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

pipeline = dlt.pipeline(
    pipeline_name="quick_start", destination="duckdb", dataset_name="testdata"
)
load_info = pipeline.run(data, table_name="users")

print(load_info)
