

with cte as (SELECT route_id, 
	recorded_at ,
	distance ,
	lag(distance) OVER(PARTITION BY route_id ORDER BY recorded_at ) AS distance_tminus,
	distance- lag(distance) OVER(PARTITION BY route_id ORDER BY recorded_at ) as diff
FROM navigation_records nr ) -- lag kullanarak t-1 deki distance yana getirdim.



,total_duration as(SELECT route_id,Cast ((
    max(JulianDay(recorded_at)) - min(JulianDay(recorded_at))
) As Integer) AS total_duration FROM cte GROUP BY route_id) --total duration ı sqllite kullandığım için julianday fonksiyonu kullandım.



,total_dis as(SELECT route_id,sum(diff) AS total_distance FROM cte GROUP BY route_id),
cte2 as(SELECT total_duration.route_id,total_dis.total_distance, total_duration.total_duration FROM total_duration  INNER JOIN total_dis ON total_dis.route_id=total_duration.route_id)

--SELECT * FROM cte WHERE diff<0 -- route da geriye giden örneklerin analizi. 
SELECT * FROM cte2 WHERE 
