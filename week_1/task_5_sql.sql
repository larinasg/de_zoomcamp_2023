select gr.passenger_count,
count(1) as cnt
from green_taxi_data gr
where gr.passenger_count in (2,3)
and gr.lpep_pickup_datetime >= timestamp'2019-01-01 00:00:00'
and gr.lpep_pickup_datetime < timestamp'2019-01-02 00:00:00'
group by gr.passenger_count