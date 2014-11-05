SELECT airline_code, COUNT(1) AS num_flights FROM flights GROUP BY airline_code ORDER BY num_flights DESC;

EXPLAIN SELECT airline_code, COUNT(1) AS num_flights FROM flights GROUP BY airline_code ORDER BY num_flights DESC;


SELECT   airline_code,
         COUNT(1) AS num_flights,
         SUM(IF(depart_delay > 0, 1, 0)) AS num_depart_delays,
         SUM(IF(arrive_delay > 0, 1, 0)) AS num_arrive_delays,
         SUM(IF(is_cancelled, 1, 0)) AS num_cancelled,
    FROM flights
    GROUP BY airline_code;


SELECT  airline_code,
        COUNT(1) AS num_flights,
        SUM(IF(depart_delay > 0, 1, 0)) AS num_depart_delays,
        ROUND(SUM(IF(depart_delay > 0, 1, 0))/COUNT(1), 2) AS depart_delay_rate,
        SUM(IF(arrive_delay > 0, 1, 0)) AS num_arrive_delays,
        ROUND(SUM(IF(arrive_delay > 0, 1, 0))/COUNT(1), 2) AS arrive_delay_rate,
        SUM(IF(is_cancelled, 1, 0)) AS num_cancelled,
        ROUND(SUM(IF(is_cancelled, 1, 0))/COUNT(1), 2) AS cancellation_rate
    FROM flights
    GROUP BY airline_code
    ORDER by cancellation_rate DESC, arrive_delay_rate DESC, depart_delay_rate DESC;


SELECT  airline_code,
        COUNT(1) AS num_flights,
        SUM(IF(depart_delay > 0, 1, 0)) AS num_depart_delays,
        ROUND(SUM(IF(depart_delay > 0, 1, 0))/COUNT(1), 2) AS depart_delay_rate,
        AVG(depart_delay) AS avg_depart_delay,
        SUM(IF(arrive_delay > 0, 1, 0)) AS num_arrive_delays,
        ROUND(SUM(IF(arrive_delay > 0, 1, 0))/COUNT(1), 2) AS arrive_delay_rate,
        AVG(arrive_delay) AS avg_arrive_delay,
        SUM(IF(is_cancelled, 1, 0)) AS num_cancelled,
        ROUND(SUM(IF(is_cancelled, 1, 0))/COUNT(1), 2) AS cancellation_rate
    FROM flights
    GROUP BY airline_code
    ORDER by cancellation_rate DESC, arrive_delay_rate DESC, depart_delay_rate DESC;


SELECT
        airlines.description,
        q.num_flights,
        q.num_depart_delays,
        q.depart_delay_rate,
        q.avg_depart_delay,
        q.num_arrive_delays,
        q.arrive_delay_rate,
        q.avg_arrive_delay,
        q.num_cancelled,
        q.cancellation_rate
        FROM
        (   SELECT
            airline_code,
            COUNT(1) AS num_flights,
            SUM(IF(depart_delay > 0, 1, 0)) AS num_depart_delays,
            ROUND(SUM(IF(depart_delay > 0, 1, 0))/COUNT(1), 2) AS depart_delay_rate,
            ROUND(AVG(depart_delay), 0) AS avg_depart_delay,
            SUM(IF(arrive_delay > 0, 1, 0)) AS num_arrive_delays,
            ROUND(SUM(IF(arrive_delay > 0, 1, 0))/COUNT(1), 2) AS arrive_delay_rate,
            ROUND(AVG(arrive_delay), 0) AS avg_arrive_delay,
            SUM(IF(is_cancelled, 1, 0)) AS num_cancelled,
            ROUND(SUM(IF(is_cancelled, 1, 0))/COUNT(1), 2) AS cancellation_rate
            FROM flights
            GROUP BY airline_code
        ) as q
    LEFT JOIN airlines ON airlines.code = q.airline_code
    ORDER by arrive_delay_rate DESC, cancellation_rate DESC, depart_delay_rate DESC;