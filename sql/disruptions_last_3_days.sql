SELECT disruption, count(*) AS journeys
FROM journeys
WHERE flight_date BETWEEN date_format(current_date - interval '3' day, '%Y-%m-%d')
    AND date_format(current_date - interval '1' day, '%Y-%m-%d')
GROUP BY 1
ORDER BY 2 DESC
