import os
import math
import random
from datetime import date, datetime, timedelta
from io import StringIO

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ----------------------------
# Config
# ----------------------------
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = "as_dw"
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = "" #put the real password later when the script is needed again

SEED = 42
fake = Faker()
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)

# Synthetic data sizes (adjust if needed)
N_CUSTOMERS = 200_000
N_MOVIES = 5_000
N_CINEMAS = 300
N_PROMOTIONS = 200
N_SHOWINGS = 72  # e.g., 24 hours * 3 periods, or keep small as time buckets

FACT_ROWS = 1_000_000
FACT_BATCH_COPY = 200_000  # rows per COPY chunk

DATE_START = date(2014, 1, 1)
DATE_END   = date(2026, 12, 31)

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera", "Brave"]
PAYMETHODS_ONLINE = ["Card", "ApplePay", "GooglePay", "PayPal"]
PAYMETHODS_OFFLINE = ["Cash", "Card"]

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
    ("Dallas", "TX"), ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
]

LANGS = ["English", "Spanish", "French", "Arabic", "Hindi", "Korean", "Japanese"]
GENRES = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Romance", "Thriller", "Animation"]
COUNTRIES = ["USA", "UK", "France", "India", "Korea", "Japan", "Canada", "Egypt"]
STARS = ["Star A", "Star B", "Star C", "Star D", "Star E"]
DIRECTORS = [f"Director {i}" for i in range(1, 401)]  # 400 directors


# ----------------------------
# Helpers
# ----------------------------
def yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day

def week_start_monday(d: date) -> date:
    # Monday=0 ... Sunday=6
    return d - timedelta(days=d.weekday())

def year_week_iso(d: date) -> int:
    # ISO week: week starts Monday
    iso_year, iso_week, _ = d.isocalendar()
    return iso_year * 100 + iso_week

def period_from_hour(h: int) -> str:
    if 6 <= h <= 11:
        return "Morning"
    if 12 <= h <= 17:
        return "Afternoon"
    return "Night"

def chunks(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


# ----------------------------
# DB
# ----------------------------
def connect():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD
    )

def fetch_all_keys(cur, table: str, key_col: str):
    cur.execute(f"SELECT {key_col} FROM {table}")
    return np.array([r[0] for r in cur.fetchall()], dtype=np.int64)

def ensure_empty(cur):
    # If you already ran your DROP+CREATE script, tables are empty.
    # This is just a safety check.
    cur.execute("SELECT COUNT(*) FROM transaction_ft")
    cnt = cur.fetchone()[0]
    if cnt != 0:
        raise RuntimeError("transaction_ft is not empty. Run your DROP+CREATE schema script first.")


# ----------------------------
# Populate dimensions
# ----------------------------
def populate_date_dt(cur):
    rows = []
    d = DATE_START
    while d <= DATE_END:
        rows.append((
            yyyymmdd(d),                  # date_id
            d,                            # full_date
            d.day,
            d.month,
            d.strftime("%B"),
            d.year,
            int(d.isocalendar().week),    # week_of_year (ISO)
            week_start_monday(d),
            year_week_iso(d),
            d.strftime("%a"),
            d.weekday() >= 5
        ))
        d += timedelta(days=1)

    sql = """
        INSERT INTO date_dt
        (date_id, full_date, day, month, month_name, year,
         week_of_year, week_start_date, year_week, day_of_week, is_weekend)
        VALUES %s
    """
    execute_values(cur, sql, rows, page_size=10_000)

def populate_customers(cur):
    genders = ["M", "F"]
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        dob = fake.date_of_birth(minimum_age=15, maximum_age=80)
        gender = random.choice(genders)
        # age_group is optional; can be derived. We'll store it anyway.
        age = (date.today() - dob).days // 365
        if age <= 20:
            ag = "0-20"
        elif age <= 30:
            ag = "21-30"
        elif age <= 40:
            ag = "31-40"
        elif age <= 50:
            ag = "41-50"
        else:
            ag = "51+"

        rows.append((
            i,                              # customer_id (OLTP)
            fake.name(),
            dob,
            fake.address().replace("\n", ", "),
            gender,
            ag
        ))

    sql = """
        INSERT INTO customer_dt
        (customer_id, name, dob, address, gender, age_group)
        VALUES %s
    """
    for batch in chunks(rows, 10_000):
        execute_values(cur, sql, batch, page_size=10_000)

def populate_movies(cur):
    rows = []
    for i in range(1, N_MOVIES + 1):
        release = fake.date_between(start_date=date(2000, 1, 1), end_date=date(2026, 12, 31))
        rows.append((
            i,  # movie_id
            release,
            random.choice(LANGS),
            random.choice(STARS),
            ", ".join(fake.name() for _ in range(random.randint(2, 6))),  # movie_cast
            random.choice(COUNTRIES),
            f"{fake.word().title()} {fake.word().title()}",
            random.choice(GENRES),
            random.choice(DIRECTORS),
        ))

    sql = """
        INSERT INTO movie_dt
        (movie_id, release_date, language, star, movie_cast, country, title, genre, director)
        VALUES %s
    """
    for batch in chunks(rows, 10_000):
        execute_values(cur, sql, batch, page_size=10_000)

def populate_cinemas(cur):
    rows = []
    for i in range(1, N_CINEMAS + 1):
        city, state = random.choice(CITIES)
        cap = random.randint(80, 350)
        if cap < 140:
            size = "Small"
        elif cap < 240:
            size = "Mid"
        else:
            size = "Large"
        rows.append((
            i,  # cinema_id
            f"Cinema {i}",
            city,
            state,
            cap,
            size,
            fake.street_address()
        ))

    sql = """
        INSERT INTO cinema_dt
        (cinema_id, cinema_name, city, state, hall_capacity, hall_size, address)
        VALUES %s
    """
    execute_values(cur, sql, rows, page_size=5_000)

def populate_promotions(cur):
    promo_types = ["None", "Student", "Weekend", "Holiday", "Member", "BOGO"]
    rows = []
    for i in range(1, N_PROMOTIONS + 1):
        ptype = random.choice(promo_types)
        discount = 0.0 if ptype == "None" else round(random.uniform(5, 35), 2)
        rows.append((
            i,  # promotion_id
            ptype,
            f"{ptype} promotion",
            discount
        ))

    sql = """
        INSERT INTO promotion_dt
        (promotion_id, promotion_type, description, discount)
        VALUES %s
    """
    execute_values(cur, sql, rows, page_size=5_000)

def populate_showings(cur):
    # Time-bucket showings (not per movie). Simple and works for analysis.
    rows = []
    sid = 1
    base_date = date(2014, 1, 1)  # show_date is not used much in DW; keeping a placeholder
    for h in range(0, 24):
        for _ in range(3):  # 3 showings per hour bucket
            stime = time_from_hour(h)
            rows.append((
                sid,
                base_date,
                stime,
                h,
                period_from_hour(h)
            ))
            sid += 1
            if len(rows) >= N_SHOWINGS:
                break
        if len(rows) >= N_SHOWINGS:
            break

    sql = """
        INSERT INTO showing_dt
        (showing_id, show_date, show_time, hour, period)
        VALUES %s
    """
    execute_values(cur, sql, rows, page_size=1_000)

def time_from_hour(h: int):
    # spread within the hour
    minute = random.choice([0, 15, 30, 45])
    return datetime(2000, 1, 1, h, minute, 0).time()


# ----------------------------
# Populate fact (1,000,000+) with COPY for speed
# ----------------------------
def populate_fact(conn, cur):
    # Fetch surrogate keys for sampling
    date_keys = fetch_all_keys(cur, "date_dt", "date_key")
    customer_keys = fetch_all_keys(cur, "customer_dt", "customer_key")
    movie_keys = fetch_all_keys(cur, "movie_dt", "movie_key")
    cinema_keys = fetch_all_keys(cur, "cinema_dt", "cinema_key")
    promo_keys = fetch_all_keys(cur, "promotion_dt", "promotion_key")
    showing_keys = fetch_all_keys(cur, "showing_dt", "showing_key")

    # We want a realistic skew:
    # - Most sales occur 2017-2020-ish, but spanning full range
    # We'll prebuild a probability distribution over dates.
    years = np.array([int(str(k)[:4]) for k in fetch_date_id_years(cur)], dtype=np.int32)

    # We'll sample date_keys uniformly over date table, but you can skew if needed.
    # For simplicity & speed: uniform sampling.
    promo_prob = 0.35  # 35% have promotions
    online_prob = 0.45  # 45% online

    # Use COPY in chunks
    cols = (
        "date_key, customer_key, movie_key, cinema_key, promotion_key, showing_key, "
        "transaction_id, paymethod, is_online, browser, tickets_sold, total_price"
    )

    inserted = 0
    txn_id_start = 10_000_000

    with conn.cursor() as c2:
        while inserted < FACT_ROWS:
            n = min(FACT_BATCH_COPY, FACT_ROWS - inserted)

            # Sample keys
            dk = np.random.choice(date_keys, size=n, replace=True)
            ck = np.random.choice(customer_keys, size=n, replace=True)
            mk = np.random.choice(movie_keys, size=n, replace=True)
            cik = np.random.choice(cinema_keys, size=n, replace=True)
            sk = np.random.choice(showing_keys, size=n, replace=True)

            # promotions (nullable)
            has_promo = np.random.random(size=n) < promo_prob
            pk = np.where(has_promo, np.random.choice(promo_keys, size=n, replace=True), None)

            # online & browser/paymethod
            is_online = np.random.random(size=n) < online_prob

            browser = np.where(is_online, np.random.choice(BROWSERS, size=n, replace=True), None)

            paymethod = np.where(
                is_online,
                np.random.choice(PAYMETHODS_ONLINE, size=n, replace=True),
                np.random.choice(PAYMETHODS_OFFLINE, size=n, replace=True),
            )

            # tickets_sold (small integers, skewed toward 1-2)
            tickets = np.random.choice([1, 2, 3, 4, 5, 6], size=n, p=[0.55, 0.25, 0.10, 0.06, 0.03, 0.01])

            # price per ticket (varies). Use lognormal-ish distribution and clamp.
            price_per_ticket = np.clip(np.random.lognormal(mean=2.3, sigma=0.25, size=n), 6, 30)
            total_price = np.round(price_per_ticket * tickets, 2)

            txn_ids = np.arange(txn_id_start + inserted, txn_id_start + inserted + n, dtype=np.int64)

            # Write CSV to memory for COPY
            buf = StringIO()
            for i in range(n):
                # promotion_key can be NULL -> represent as \N for COPY
                promo_val = "\\N" if pk[i] is None else str(int(pk[i]))
                show_val = str(int(sk[i]))
                brow_val = "\\N" if browser[i] is None else str(browser[i])
                line = (
                    f"{int(dk[i])}\t{int(ck[i])}\t{int(mk[i])}\t{int(cik[i])}\t"
                    f"{promo_val}\t{show_val}\t{int(txn_ids[i])}\t{paymethod[i]}\t"
                    f"{'t' if is_online[i] else 'f'}\t{brow_val}\t{int(tickets[i])}\t{float(total_price[i])}\n"
                )
                buf.write(line)
            buf.seek(0)

            copy_sql = f"""
                COPY transaction_ft ({cols})
                FROM STDIN WITH (FORMAT text)
            """
            c2.copy_expert(copy_sql, buf)
            conn.commit()

            inserted += n
            print(f"Inserted {inserted:,}/{FACT_ROWS:,} fact rows...")

def fetch_date_id_years(cur):
    cur.execute("SELECT date_id FROM date_dt")
    return [r[0] for r in cur.fetchall()]


# ----------------------------
# Main
# ----------------------------
def main():
    print("Connecting to Postgres...")
    conn = connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            ensure_empty(cur)

            print("Populating date_dt...")
            populate_date_dt(cur)
            conn.commit()

            print("Populating customer_dt...")
            populate_customers(cur)
            conn.commit()

            print("Populating movie_dt...")
            populate_movies(cur)
            conn.commit()

            print("Populating cinema_dt...")
            populate_cinemas(cur)
            conn.commit()

            print("Populating promotion_dt...")
            populate_promotions(cur)
            conn.commit()

            print("Populating showing_dt...")
            populate_showings(cur)
            conn.commit()

            print("Populating transaction_ft (>= 1,000,000 rows)...")
            populate_fact(conn, cur)

            # quick screenshot-friendly row counts
            cur.execute("""
                SELECT
                  (SELECT COUNT(*) FROM date_dt)       AS date_rows,
                  (SELECT COUNT(*) FROM customer_dt)   AS customer_rows,
                  (SELECT COUNT(*) FROM movie_dt)      AS movie_rows,
                  (SELECT COUNT(*) FROM cinema_dt)     AS cinema_rows,
                  (SELECT COUNT(*) FROM promotion_dt)  AS promo_rows,
                  (SELECT COUNT(*) FROM showing_dt)    AS showing_rows,
                  (SELECT COUNT(*) FROM transaction_ft)AS fact_rows
            """)
            print("Final counts:", cur.fetchone())
            conn.commit()

        print("Done ✅")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
