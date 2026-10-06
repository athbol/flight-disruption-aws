SELECT flight_no, origin, destination, scheduled_departure, flight_status, delay_minutes
FROM flights
WHERE flight_date = date_format(current_date - interval '1' day, '%Y-%m-%d') AND delay_minutes > 0
ORDER BY delay_minutes DESC
