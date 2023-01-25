select date_trunc('day',gr.lpep_pickup_datetime) as pickup_day,
max(gr.trip_distance) max_distance
from green_taxi_data gr
group by pickup_day
order by max_distance desc
limit 1