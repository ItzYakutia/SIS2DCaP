-- Checks we run after loading. loader.py runs them, or by hand:
-- sqlite3 data/output.db < sql/verify_queries.sql

-- how many rows are stored
SELECT COUNT(*) AS stored_rows FROM race_results;

-- rows per season
SELECT season, COUNT(*) AS rows, COUNT(DISTINCT round) AS races FROM race_results GROUP BY season;

-- ids that appear more than once (should be empty)
SELECT record_id, COUNT(*) AS times FROM race_results GROUP BY record_id HAVING COUNT(*) > 1;

-- rows with missing required values (should be 0)
SELECT COUNT(*) AS rows_with_missing FROM race_results
WHERE driver_name IS NULL OR constructor_name IS NULL OR race_date IS NULL;

-- top 5 race winners
SELECT driver_name, COUNT(*) AS wins FROM race_results WHERE position = 1
GROUP BY driver_name ORDER BY wins DESC LIMIT 5;

-- constructor champions by points per season
SELECT season, constructor_name, SUM(points) AS points FROM race_results
GROUP BY season, constructor_name ORDER BY season, points DESC;
