CREATE OR REPLACE EXTERNAL TABLE `starry-hawk-374910.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_starry-hawk-374910/data/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE starry-hawk-374910.trips_data_all.fhv_tripdata
AS
SELECT * FROM starry-hawk-374910.trips_data_all.external_fhv_tripdata;

SELECT count(1) FROM starry-hawk-374910.trips_data_all.fhv_tripdata

SELECT count(distinct d.Affiliated_base_number)
FROM starry-hawk-374910.trips_data_all.fhv_tripdata d

SELECT count(distinct d.Affiliated_base_number)
FROM starry-hawk-374910.trips_data_all.external_fhv_tripdata d

SELECT count(1)
FROM starry-hawk-374910.trips_data_all.fhv_tripdata d
where d.PUlocationID is null 
and d.DOlocationID is null

CREATE OR REPLACE TABLE starry-hawk-374910.trips_data_all.fhv_tripdata_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS
SELECT * FROM starry-hawk-374910.trips_data_all.fhv_tripdata;

select count(distinct c.affiliated_base_number)
from starry-hawk-374910.trips_data_all.fhv_tripdata_partitioned_clustered c
where c.pickup_datetime >= timestamp'2019-03-01 00:00:00'
and c.pickup_datetime < timestamp'2019-04-01 00:00:00'

select count(distinct c.affiliated_base_number)
from starry-hawk-374910.trips_data_all.fhv_tripdata c
where c.pickup_datetime >= timestamp'2019-03-01 00:00:00'
and c.pickup_datetime < timestamp'2019-04-01 00:00:00'

CREATE OR REPLACE EXTERNAL TABLE `starry-hawk-374910.trips_data_all.parquet_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_starry-hawk-374910/data/fhv_tripdata_2019-*.parquet']
);
