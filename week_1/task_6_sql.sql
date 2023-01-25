select dz."Zone" as dropoff_zone,
max(gr.tip_amount) as max_tip
from green_taxi_data gr
join zones pz on gr."PULocationID" = pz."LocationID"
				 and pz."Zone" = 'Astoria'
join zones dz on gr."DOLocationID" = dz."LocationID"	
group by dz."Zone"
order by max_tip desc
limit 1