SELECT *
FROM journeys
WHERE passenger_id = ?
ORDER BY flight_date, scheduled_departure
