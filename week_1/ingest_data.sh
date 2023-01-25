docker run -it \
	--network=pg-network \
taxi_ingest:v001 \
	--user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_data \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz