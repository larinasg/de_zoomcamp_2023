select count(1) from green_taxi_data gr
where gr.lpep_pickup_datetime >= timestamp'2019-01-15 00:00:00'
and gr.lpep_dropoff_datetime < timestamp'2019-01-16 00:00:00'