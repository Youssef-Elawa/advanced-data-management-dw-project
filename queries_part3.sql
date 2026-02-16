-- =========================================================
-- Advanced Data Management - Midterm Project Part 3
-- Queries 11 to 18 (PostgreSQL)
-- Schema uses:
--   transaction_ft.date_key -> date_dt.date_key
-- =========================================================


-- ---------------------------------------------------------
-- Q11. For each city, rank the cinemas in descending order
--      of total sales in 2018.
-- ---------------------------------------------------------
WITH city_cinema_sales AS (
  SELECT
    ci.city,
    ci.cinema_name,
    SUM(f.total_price) AS total_sales
  FROM transaction_ft f
  JOIN date_dt d     ON d.date_key = f.date_key
  JOIN cinema_dt ci  ON ci.cinema_key = f.cinema_key
  WHERE d.year = 2018
  GROUP BY ci.city, ci.cinema_name
)
SELECT
  city,
  cinema_name,
  total_sales,
  RANK() OVER (PARTITION BY city ORDER BY total_sales DESC) AS cinema_rank
FROM city_cinema_sales
ORDER BY city, cinema_rank, cinema_name;


-- ---------------------------------------------------------
-- Q12. For each director, rank movies by total sales
--      for customers with age < 40 at time of purchase.
-- ---------------------------------------------------------
WITH director_movie_sales AS (
  SELECT
    m.director,
    m.title,
    SUM(f.total_price) AS total_sales
  FROM transaction_ft f
  JOIN date_dt d      ON d.date_key = f.date_key
  JOIN customer_dt c  ON c.customer_key = f.customer_key
  JOIN movie_dt m     ON m.movie_key = f.movie_key
  WHERE EXTRACT(YEAR FROM AGE(d.full_date, c.dob)) < 40
  GROUP BY m.director, m.title
)
SELECT
  director,
  title,
  total_sales,
  RANK() OVER (PARTITION BY director ORDER BY total_sales DESC) AS movie_rank
FROM director_movie_sales
ORDER BY director, movie_rank, title;


-- ---------------------------------------------------------
-- Q13. For each city, rank browsers in descending order
--      of total number of ONLINE transactions.
--      (Mentions states in the question; state is available
--       in cinema_dt and could be added if needed.)
-- ---------------------------------------------------------
WITH city_browser_txn AS (
  SELECT
    ci.city,
    f.browser,
    COUNT(*) AS txn_count
  FROM transaction_ft f
  JOIN cinema_dt ci ON ci.cinema_key = f.cinema_key
  WHERE f.is_online = TRUE
    AND f.browser IS NOT NULL
  GROUP BY ci.city, f.browser
)
SELECT
  city,
  browser,
  txn_count,
  RANK() OVER (PARTITION BY city ORDER BY txn_count DESC) AS browser_rank
FROM city_browser_txn
ORDER BY city, browser_rank, browser;


-- ---------------------------------------------------------
-- Q14. Top 10 movies in 2018 by total tickets sold
--      for male and female customers, respectively.
-- ---------------------------------------------------------
WITH gender_movie_tickets AS (
  SELECT
    c.gender,
    m.title,
    SUM(f.tickets_sold) AS tickets_sold
  FROM transaction_ft f
  JOIN date_dt d      ON d.date_key = f.date_key
  JOIN customer_dt c  ON c.customer_key = f.customer_key
  JOIN movie_dt m     ON m.movie_key = f.movie_key
  WHERE d.year = 2018
    AND c.gender IN ('M', 'F')
  GROUP BY c.gender, m.title
),
ranked AS (
  SELECT
    gender,
    title,
    tickets_sold,
    DENSE_RANK() OVER (PARTITION BY gender ORDER BY tickets_sold DESC) AS movie_rank
  FROM gender_movie_tickets
)
SELECT
  gender,
  title,
  tickets_sold,
  movie_rank
FROM ranked
WHERE movie_rank <= 10
ORDER BY gender, movie_rank, title;


-- ---------------------------------------------------------
-- Q15. For each city, top 5 cinemas by total tickets sold
--      from 2014 to 2018.
-- ---------------------------------------------------------
WITH city_cinema_tickets AS (
  SELECT
    ci.city,
    ci.cinema_name,
    SUM(f.tickets_sold) AS tickets_sold
  FROM transaction_ft f
  JOIN date_dt d     ON d.date_key = f.date_key
  JOIN cinema_dt ci  ON ci.cinema_key = f.cinema_key
  WHERE d.year BETWEEN 2014 AND 2018
  GROUP BY ci.city, ci.cinema_name
),
ranked AS (
  SELECT
    city,
    cinema_name,
    tickets_sold,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY tickets_sold DESC) AS cinema_rank
  FROM city_cinema_tickets
)
SELECT
  city,
  cinema_name,
  tickets_sold,
  cinema_rank
FROM ranked
WHERE cinema_rank <= 5
ORDER BY city, cinema_rank, cinema_name;


-- ---------------------------------------------------------
-- Q16. Compute the 8-week moving average of total sales
--      for each week in 2018.
-- Notes:
-- - Uses date_dt.year_week (YYYYWW) for ordering
-- - Aggregates sales per week then applies window average
-- ---------------------------------------------------------
WITH weekly_sales AS (
  SELECT
    d.year_week,
    MIN(d.week_start_date) AS week_start_date,
    SUM(f.total_price) AS week_sales
  FROM transaction_ft f
  JOIN date_dt d ON d.date_key = f.date_key
  WHERE d.year = 2018
  GROUP BY d.year_week
)
SELECT
  year_week,
  week_start_date,
  week_sales,
  AVG(week_sales) OVER (
    ORDER BY year_week
    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
  ) AS ma_8week
FROM weekly_sales
ORDER BY year_week;


-- ---------------------------------------------------------
-- Q17. Largest three 4-week moving averages of total sales
--      among the weeks in 2018.
-- ---------------------------------------------------------
WITH weekly_sales AS (
  SELECT
    d.year_week,
    MIN(d.week_start_date) AS week_start_date,
    SUM(f.total_price) AS week_sales
  FROM transaction_ft f
  JOIN date_dt d ON d.date_key = f.date_key
  WHERE d.year = 2018
  GROUP BY d.year_week
),
ma AS (
  SELECT
    year_week,
    week_start_date,
    AVG(week_sales) OVER (
      ORDER BY year_week
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS ma_4week
  FROM weekly_sales
)
SELECT
  year_week,
  week_start_date,
  ma_4week
FROM ma
ORDER BY ma_4week DESC
LIMIT 3;


-- ---------------------------------------------------------
-- Q18. For each city, largest 4-week moving average of total
--      sales from 2010 to 2018.
-- Note: Your synthetic data starts 2014, so results will
--       effectively cover 2014–2018 (no error).
-- ---------------------------------------------------------
WITH city_weekly_sales AS (
  SELECT
    ci.city,
    d.year_week,
    MIN(d.week_start_date) AS week_start_date,
    SUM(f.total_price) AS week_sales
  FROM transaction_ft f
  JOIN date_dt d    ON d.date_key = f.date_key
  JOIN cinema_dt ci ON ci.cinema_key = f.cinema_key
  WHERE d.year BETWEEN 2010 AND 2018
  GROUP BY ci.city, d.year_week
),
city_ma AS (
  SELECT
    city,
    year_week,
    week_start_date,
    AVG(week_sales) OVER (
      PARTITION BY city
      ORDER BY year_week
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS ma_4week
  FROM city_weekly_sales
),
city_max AS (
  SELECT
    city,
    MAX(ma_4week) AS max_4week_ma
  FROM city_ma
  GROUP BY city
)
SELECT
  city,
  max_4week_ma
FROM city_max
ORDER BY max_4week_ma DESC;
