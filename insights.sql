-- 1. Worst routes
SELECT 
    route_name,
    COUNT(*) as observations,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delay_minutes) as median_delay
FROM delay_observations
GROUP BY route_name
HAVING COUNT(*) > 200
ORDER BY avg_delay DESC
LIMIT 10;

-- 2. Worst stations
SELECT 
    s.name as station_name,
    COUNT(*) as observations,
    ROUND(AVG(d.delay_minutes)::numeric,2) as avg_delay,
    MAX(d.delay_minutes) as worst_delay
FROM delay_observations d
JOIN stations s ON d.station_id = s.id
GROUP BY s.name
HAVING COUNT(*) > 200
ORDER BY avg_delay DESC
LIMIT 10;

-- 3. Extreme delays (top 10 worst events)
SELECT 
    route_name,
    s.name as station_name,
    delay_minutes,
    actual_arrival
FROM delay_observations d
JOIN stations s ON d.station_id = s.id
ORDER BY delay_minutes DESC
LIMIT 10;

-- 4. On-time %
SELECT 
    ROUND(
        COUNT(*) FILTER (WHERE delay_minutes <= 5) * 100.0 / COUNT(*)::numeric,
        2
    ) as on_time_percentage
FROM delay_observations;

-- 5. Delay distribution
SELECT 
    CASE 
        WHEN delay_minutes <= 5 THEN 'On time (0-5)'
        WHEN delay_minutes <= 15 THEN 'Minor delay (6-15)'
        ELSE 'Major delay (16+)'
    END as category,
    COUNT(*) as count
FROM delay_observations
GROUP BY category
ORDER BY count DESC;

-- 6. Hour pattern
SELECT 
    EXTRACT(HOUR FROM actual_arrival) as hour,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay,
    COUNT(*) as observations
FROM delay_observations
GROUP BY hour
ORDER BY avg_delay DESC;

-- 7. Day pattern
SELECT 
    EXTRACT(DOW FROM actual_arrival) as day,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay,
    COUNT(*) as observations
FROM delay_observations
GROUP BY day
ORDER BY avg_delay DESC;

-- 8. Most unpredictable routes (variance)
SELECT 
    route_name,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay,
    ROUND(STDDEV(delay_minutes)::numeric,2) as variability
FROM delay_observations
GROUP BY route_name
HAVING COUNT(*) > 200
ORDER BY variability DESC
LIMIT 10;

-- 9. Worst station-route combos
SELECT 
    s.name as station_name,
    route_name,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay,
    COUNT(*) as observations
FROM delay_observations d
JOIN stations s ON d.station_id = s.id
GROUP BY s.name, route_name
HAVING COUNT(*) > 50
ORDER BY avg_delay DESC
LIMIT 10;

-- 10. Routes with biggest single delay
SELECT 
    route_name,
    MAX(delay_minutes) as worst_delay
FROM delay_observations
GROUP BY route_name
ORDER BY worst_delay DESC
LIMIT 10;

-- 11. Weekly trend
SELECT 
    route_name,
    DATE_TRUNC('week', actual_arrival) as week,
    ROUND(AVG(delay_minutes)::numeric,2) as avg_delay
FROM delay_observations
GROUP BY route_name, week
ORDER BY route_name, week;

-- 12. Top routes by volume
SELECT 
    route_name,
    COUNT(*) as observations
FROM delay_observations
GROUP BY route_name
ORDER BY observations DESC
LIMIT 10;
