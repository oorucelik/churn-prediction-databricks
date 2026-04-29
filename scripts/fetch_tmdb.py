import os
import requests
import time
import json
import random
import numpy as np
import pandas as pd
from databricks import sql as databricks_sql
from datetime import datetime, timezone, timedelta
from faker import Faker
from pathlib import Path

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


# ─── CONSTANTS ───────────────────────────────────────────────────────────────

PLANS = ["basic", "standard", "premium"]
PLAN_PRICES = {"basic": 9.99, "standard": 15.49, "premium": 22.99}

# basic=1 screen, standard=2, premium=4 — affects concurrent watch behavior
PLAN_SCREENS = {"basic": 1, "standard": 2, "premium": 4}

CANCEL_REASONS = [
    "too_expensive",
    "not_enough_content",
    "switching_competitor",
    "technical_issues",
    "personal_reasons",
]
SUPPORT_CATEGORIES = ["billing", "technical", "content", "account"]
SUPPORT_CHANNELS = ["chat", "email", "phone"]
DEVICES = ["smart_tv", "mobile", "tablet", "desktop", "gaming_console"]
ACQUISITION_CHANNELS = ["organic", "paid_search", "social", "referral", "tv_ad"]

# TMDB genre IDs → names (used when API isn't available)
TMDB_GENRES = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
    10759: "Action & Adventure", 10762: "Kids", 10763: "News",
    10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap",
    10767: "Talk", 10768: "War & Politics",
}

CACHE_DIR = Path(__file__).parent / ".cache"
# Inside Docker the working dir is /app, so seeds land at /app/seeds.
# Locally (running from repo root) they land at seeds/ — same as before.
SEEDS_DIR = Path(os.environ.get("SEEDS_DIR", Path(__file__).parent.parent / "seeds"))
SEEDS_DIR.mkdir(parents=True, exist_ok=True)

def fetch_tmdb_content(api_key: str, movie_pages: int = 25, tv_pages: int = 10):
    """
    Fetch popular movies and TV shows from TMDB API.
    Returns a DataFrame with unified content catalog.
    """
    base_url = "https://api.themoviedb.org/3"
    headers = {"Authorization": f"Bearer {api_key}"}
    all_content = []

    # Fetch popular movies
    print(f"  Fetching {movie_pages} pages of popular movies from TMDB...")
    for page in range(1, movie_pages + 1):
        popular_movies_resp = requests.get(
            f"{base_url}/movie/popular",
            headers=headers,
            params={"page": page, "language": "en-US"},
        )
        popular_movies_resp.raise_for_status()
        for movie in popular_movies_resp.json()["results"]:
            genre_names = [TMDB_GENRES.get(g, "Unknown") for g in movie.get("genre_ids", [])]
            all_content.append({
                "content_id": f"TMDB_M_{movie['id']}",
                "tmdb_id": movie["id"],
                "title": movie["title"],
                "content_type": "movie",
                "genres": "|".join(genre_names),
                "primary_genre": genre_names[0] if genre_names else "Unknown",
                "release_date": movie.get("release_date", None),
                "vote_average": movie.get("vote_average", 0),
                "vote_count": movie.get("vote_count", 0),
                "popularity": movie.get("popularity", 0),
                "original_language": movie.get("original_language", "en"),
                "overview": (movie.get("overview", "") or "")[:200],
            })
            # Fetch runtime from detail endpoint
            time.sleep(0.3)
            detail_resp = requests.get(
                f"{base_url}/movie/{movie['id']}",
                headers=headers,
                params={"language": "en-US"},
            )
            detail_resp.raise_for_status()
            all_content[-1]["runtime_minutes"] = detail_resp.json().get("runtime") or None
        time.sleep(0.3)

    # Fetch popular TV shows
    print(f"  Fetching {tv_pages} pages of popular TV shows from TMDB...")
    for page in range(1, tv_pages + 1):
        popular_tv_shows_resp = requests.get(
            f"{base_url}/tv/popular",
            headers=headers,
            params={"page": page, "language": "en-US"},
        )
        popular_tv_shows_resp.raise_for_status()
        for show in popular_tv_shows_resp.json()["results"]:
            genre_names = [TMDB_GENRES.get(g, "Unknown") for g in show.get("genre_ids", [])]
            all_content.append({
                "content_id": f"TMDB_T_{show['id']}",
                "tmdb_id": show["id"],
                "title": show["name"],
                "content_type": "tv_show",
                "genres": "|".join(genre_names),
                "primary_genre": genre_names[0] if genre_names else "Unknown",
                "release_date": show.get("first_air_date", None),
                "vote_average": show.get("vote_average", 0),
                "vote_count": show.get("vote_count", 0),
                "popularity": show.get("popularity", 0),
                "original_language": show.get("original_language", "en"),
                "overview": (show.get("overview", "") or "")[:200],
            })
            # Fetch episode runtime from detail endpoint
            time.sleep(0.3)
            detail_resp = requests.get(
                f"{base_url}/tv/{show['id']}",
                headers=headers,
                params={"language": "en-US"},
            )
            detail_resp.raise_for_status()
            runtimes = detail_resp.json().get("episode_run_time", [])
            all_content[-1]["runtime_minutes"] = runtimes[0] if runtimes else None
        time.sleep(0.3)

    return pd.DataFrame(all_content)

def generate_users(n: int, content_df: pd.DataFrame):
    """
    Generate users with genre preferences (for realistic watch patterns).
    Each user gets 2-4 preferred genres that drive their watch behavior.
    """
    all_genres = list(set(
        g for genres_str in content_df["primary_genre"].unique()
        for g in [genres_str] if g != "Unknown"
    ))

    users = []
    for i in range(n):
        signup = fake.date_between(start_date="-2y", end_date="today")
        plan = random.choice(PLANS)
        is_churned = random.random() < 0.15  # 15% churn rate

        # Assign 2-4 preferred genres per user (drives recommendations)
        preferred = random.sample(all_genres, k=min(random.randint(2, 4), len(all_genres)))

        # Active days: how many days per week they typically watch
        # Churned users tend to have been less active before churning
        if is_churned:
            activity_level = random.choices(
                ["low", "medium", "high"], weights=[50, 35, 15]
            )[0]
        else:
            activity_level = random.choices(
                ["low", "medium", "high"], weights=[20, 40, 40]
            )[0]

        users.append({
            "customer_id": f"CUST_{i:06d}",
            "email": fake.email(),
            "country": fake.country_code(),
            "age": random.randint(18, 70),
            "signup_date": signup,
            "plan_type": plan,
            "monthly_revenue": PLAN_PRICES[plan],
            "is_churned": is_churned,
            "churn_date": (
                signup + timedelta(days=random.randint(30, 365))
                if is_churned else None
            ),
            "acquisition_channel": random.choice(ACQUISITION_CHANNELS),
            "device_type": random.choice(DEVICES),
            "preferred_genres": "|".join(preferred),
            "activity_level": activity_level,
        })

    return pd.DataFrame(users)


# ─── SUBSCRIPTION EVENTS ─────────────────────────────────────────────────────

def generate_subscription_events(users_df: pd.DataFrame):
    """Generate subscription lifecycle events (same logic as before)."""
    events = []
    for _, user in users_df.iterrows():
        events.append({
            "customer_id": user["customer_id"],
            "event_type": "subscribe",
            "event_date": user["signup_date"],
            "plan_type": user["plan_type"],
            "monthly_revenue": user["monthly_revenue"],
            "cancellation_reason": None,
        })

        if random.random() < 0.3:
            new_plan = random.choice(PLANS)
            events.append({
                "customer_id": user["customer_id"],
                "event_type": (
                    "upgrade" if PLAN_PRICES[new_plan] > user["monthly_revenue"]
                    else "downgrade"
                ),
                "event_date": user["signup_date"] + timedelta(days=random.randint(30, 180)),
                "plan_type": new_plan,
                "monthly_revenue": PLAN_PRICES[new_plan],
                "cancellation_reason": None,
            })

        if user["is_churned"]:
            events.append({
                "customer_id": user["customer_id"],
                "event_type": "cancel",
                "event_date": user["churn_date"],
                "plan_type": user["plan_type"],
                "monthly_revenue": 0,
                "cancellation_reason": random.choice(CANCEL_REASONS),
            })

    return pd.DataFrame(events)


# ─── WATCH EVENTS (THE BIG ONE) ─────────────────────────────────────────────

def generate_watch_events(users_df: pd.DataFrame, content_df: pd.DataFrame):
    """
    Generate realistic watch history for each user.

    Behavior patterns:
    - Users watch more content that matches their preferred genres
    - Churning users show declining watch frequency over time
    - Binge watchers (20% of users) watch multiple episodes in one session
    - Completion rates vary: some abandon content, some finish everything
    - Higher-rated content gets watched more (popularity bias)
    """
    activity_watches_per_week = {"low": (1, 2), "medium": (2, 5), "high": (5, 10)}
    events = []
    event_counter = 0

    # Pre-compute content pools by genre for fast lookup
    genre_content = {}
    for genre in content_df["primary_genre"].unique():
        genre_content[genre] = content_df[content_df["primary_genre"] == genre].index.tolist()

    all_content_indices = content_df.index.tolist()
    # Weight by popularity for random selection
    popularity_weights = content_df["popularity"].values
    popularity_weights = popularity_weights / popularity_weights.sum()

    for _, user in users_df.iterrows():
        signup = pd.to_datetime(user["signup_date"])
        end_date = pd.to_datetime(user["churn_date"]) if user["is_churned"] else pd.Timestamp.now()
        active_days = (end_date - signup).days

        if active_days <= 0:
            continue

        preferred = user["preferred_genres"].split("|")
        min_w, max_w = activity_watches_per_week[user["activity_level"]]
        is_binge_watcher = random.random() < 0.20
        user_device = user["device_type"]

        # Total watches during active period
        total_weeks = max(active_days / 7, 1)
        total_watches = int(total_weeks * random.uniform(min_w, max_w))
        total_watches = min(total_watches, 100)  # cap per user for seed-friendly size

        # Track what this user has watched (no exact duplicates)
        watched_content = set()

        for w in range(total_watches):
            event_counter += 1

            # Determine watch date — churners show declining frequency
            if user["is_churned"] and w > total_watches * 0.6:
                # Last 40% of watches are spread more thinly (engagement decay)
                day_offset = random.randint(int(active_days * 0.6), active_days)
            else:
                day_offset = random.randint(0, active_days)

            watch_date = signup + timedelta(days=day_offset)

            # Pick content: 70% chance from preferred genres, 30% discovery
            if random.random() < 0.70 and preferred:
                genre = random.choice(preferred)
                pool = genre_content.get(genre, all_content_indices)
                if pool:
                    idx = random.choice(pool)
                else:
                    idx = np.random.choice(all_content_indices, p=popularity_weights)
            else:
                idx = np.random.choice(all_content_indices, p=popularity_weights)

            content = content_df.iloc[idx]
            runtime = content["runtime_minutes"]
            # Guard: NaN when TMDB has no runtime data (common for TV shows)
            if pd.isna(runtime) or runtime <= 0:
                runtime = 45 if content["content_type"] == "tv_show" else 90

            # Completion: binge watchers finish more, churners finish less
            if user["is_churned"] and day_offset > active_days * 0.7:
                completion_pct = round(random.uniform(0.10, 0.50), 2)
            elif is_binge_watcher:
                completion_pct = round(random.uniform(0.75, 1.00), 2)
            else:
                completion_pct = round(random.uniform(0.20, 1.00), 2)

            watch_duration = int(runtime * completion_pct)

            # Device: mostly their primary, sometimes switch
            device = user_device if random.random() < 0.75 else random.choice(DEVICES)

            # Session: binge watchers get same session_id for consecutive watches
            if is_binge_watcher and w > 0 and random.random() < 0.6:
                session_id = events[-1]["session_id"]  # continue session
            else:
                session_id = f"SESS_{fake.uuid4()[:8].upper()}"

            events.append({
                "watch_event_id": f"WE_{event_counter:08d}",
                "customer_id": user["customer_id"],
                "content_id": content["content_id"],
                "watch_date": watch_date.strftime("%Y-%m-%d"),
                "watch_start_time": f"{random.randint(6, 23):02d}:{random.randint(0, 59):02d}:00",
                "watch_duration_minutes": watch_duration,
                "completion_percentage": completion_pct,
                "device_type": device,
                "session_id": session_id,
                "is_resumed": random.random() < 0.15,  # 15% resume from where they left off
            })

            watched_content.add(content["content_id"])

    return pd.DataFrame(events)


# ─── USER RATINGS ────────────────────────────────────────────────────────────

def generate_user_ratings(watch_events_df: pd.DataFrame):
    """
    Generate ratings for ~30% of watched content.
    Rating correlates with completion: finished content gets higher ratings.
    """
    ratings = []
    rating_counter = 0

    # Group by user+content to get unique user-content pairs
    unique_watches = (
        watch_events_df
        .groupby(["customer_id", "content_id"])
        .agg(
            max_completion=("completion_percentage", "max"),
            last_watch_date=("watch_date", "max"),
        )
        .reset_index()
    )

    for _, row in unique_watches.iterrows():
        if random.random() > 0.30:  # only 30% of watches get rated
            continue

        rating_counter += 1
        completion = row["max_completion"]

        # Rating correlates with completion (finished = higher rating)
        if completion >= 0.90:
            rating = random.choices([3, 4, 5], weights=[15, 35, 50])[0]
        elif completion >= 0.50:
            rating = random.choices([2, 3, 4], weights=[20, 45, 35])[0]
        else:
            rating = random.choices([1, 2, 3], weights=[40, 40, 20])[0]

        ratings.append({
            "rating_id": f"RAT_{rating_counter:07d}",
            "customer_id": row["customer_id"],
            "content_id": row["content_id"],
            "rating": rating,
            "rated_at": row["last_watch_date"],
        })

    return pd.DataFrame(ratings)


# ─── WATCHLIST ───────────────────────────────────────────────────────────────

def generate_watchlist(
    users_df: pd.DataFrame,
    content_df: pd.DataFrame,
    watch_events_df: pd.DataFrame,
):
    """
    Generate per-user watchlists (saved-for-later content).

    Patterns:
    - Users add content from preferred genres they haven't watched yet
    - ~40% of watchlist items eventually get watched (conversion metric)
    - Churned users have lower watchlist-to-watch conversion
    """
    watchlist = []
    wl_counter = 0

    # Build watched-content set per user for fast lookup
    watched_per_user = (
        watch_events_df
        .groupby("customer_id")["content_id"]
        .apply(set)
        .to_dict()
    )

    all_content_ids = content_df["content_id"].tolist()
    content_by_genre = content_df.groupby("primary_genre")["content_id"].apply(list).to_dict()

    for _, user in users_df.iterrows():
        n_watchlist = random.randint(3, 25)
        preferred = user["preferred_genres"].split("|")
        watched = watched_per_user.get(user["customer_id"], set())
        signup = pd.to_datetime(user["signup_date"])
        end_date = pd.to_datetime(user["churn_date"]) if user["is_churned"] else pd.Timestamp.now()

        for _ in range(n_watchlist):
            # Pick from preferred genres content they haven't watched
            if random.random() < 0.75 and preferred:
                genre = random.choice(preferred)
                pool = content_by_genre.get(genre, all_content_ids)
            else:
                pool = all_content_ids

            content_id = random.choice(pool)

            # Did they eventually watch it?
            if content_id in watched:
                is_watched = True
            elif user["is_churned"]:
                is_watched = random.random() < 0.20  # churners convert less
            else:
                is_watched = random.random() < 0.45

            active_days = max((end_date - signup).days, 1)
            added_at = signup + timedelta(days=random.randint(0, active_days))

            wl_counter += 1
            watchlist.append({
                "watchlist_id": f"WL_{wl_counter:07d}",
                "customer_id": user["customer_id"],
                "content_id": content_id,
                "added_at": added_at.strftime("%Y-%m-%d"),
                "is_watched": is_watched,
            })

    return pd.DataFrame(watchlist)


# ─── SUPPORT TICKETS ─────────────────────────────────────────────────────────

def generate_support_tickets(users_df: pd.DataFrame, n_tickets: int = 5_000):
    """Generate support tickets (same logic as before)."""
    tickets = []
    for _ in range(n_tickets):
        user = users_df.sample(1).iloc[0]
        tickets.append({
            "ticket_id": f"TKT_{fake.uuid4()[:8].upper()}",
            "customer_id": user["customer_id"],
            "category": random.choice(SUPPORT_CATEGORIES),
            "channel": random.choice(SUPPORT_CHANNELS),
            "created_at": fake.date_time_between(
                start_date=user["signup_date"], end_date="now"
            ),
            "resolution_time_hours": round(random.uniform(0.5, 72), 1),
            "is_resolved": random.random() < 0.85,
            "sentiment_score": round(random.uniform(-1, 1), 2),
        })
    return pd.DataFrame(tickets)

def write_to_databricks(
    df: pd.DataFrame,
    host: str,
    http_path: str,
    token: str,
    catalog: str = "prod",
    schema: str = "dbo_raw",
    table: str = "raw_content_catalog",
) -> None:
    """
    Full-refresh write of the content catalog DataFrame to Databricks Unity Catalog.
    Adds _loaded_at timestamp for dbt source freshness checks.
    """
    loaded_at = datetime.now(timezone.utc)
    df = df.copy()
    df["_loaded_at"] = loaded_at

    # Coerce types
    df["tmdb_id"] = df["tmdb_id"].astype("Int64")
    df["vote_count"] = df["vote_count"].astype("Int64")
    df["runtime_minutes"] = pd.to_numeric(df["runtime_minutes"], errors="coerce").astype("Int64")
    df = df.where(pd.notna(df), None)  # convert NaN → None for SQL NULL

    full_table = f"`{catalog}`.`{schema}`.`{table}`"

    print(f"  Connecting to {host}...")
    with databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            # Ensure schema exists
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

            # Full refresh: drop and recreate
            cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
            cursor.execute(f"""
                CREATE TABLE {full_table} (
                    content_id        STRING,
                    tmdb_id           LONG,
                    title             STRING,
                    content_type      STRING,
                    genres            STRING,
                    primary_genre     STRING,
                    release_date      STRING,
                    vote_average      DOUBLE,
                    vote_count        LONG,
                    popularity        DOUBLE,
                    original_language STRING,
                    overview          STRING,
                    runtime_minutes   LONG,
                    _loaded_at        TIMESTAMP
                ) USING DELTA
            """)
            print(f"  Created {full_table}")

            # Build inline INSERT statements (avoids connector parameterization issues)
            def sql_val(val):
                """Convert a Python value to a SQL literal."""
                if val is None:
                    return "NULL"
                if hasattr(val, 'item'):  # numpy scalar → Python
                    val = val.item()
                if pd.isna(val):
                    return "NULL"
                if isinstance(val, (int, float)):
                    return str(val)
                if isinstance(val, datetime):
                    return f"'{val.isoformat()}'"
                # String: escape single quotes
                return "'" + str(val).replace("'", "''") + "'"

            chunk_size = 100
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                values_list = []
                for _, row in chunk.iterrows():
                    vals = ", ".join(sql_val(v) for v in row)
                    values_list.append(f"({vals})")
                insert_sql = f"INSERT INTO {full_table} VALUES {', '.join(values_list)}"
                cursor.execute(insert_sql)
                print(f"  Inserted rows {i + 1}–{min(i + chunk_size, len(df))} / {len(df)}")

    print(f"  Done — {len(df)} rows written to {full_table} at {loaded_at.isoformat()}")


if __name__ == "__main__":
    # ── Read env vars ─────────────────────────────────────────────────────────
    api_key      = os.environ["TMDB_ACCESS_TOKEN"]
    db_host      = os.environ["DATABRICKS_HOST"]
    db_http_path = os.environ["DATABRICKS_HTTP_PATH"]
    db_token     = os.environ["DATABRICKS_TOKEN"]
    movie_pages  = int(os.environ.get("TMDB_MOVIE_PAGES", "1"))
    tv_pages     = int(os.environ.get("TMDB_TV_PAGES", "1"))
    n_users      = int(os.environ.get("N_USERS", "1000"))
    n_tickets    = int(os.environ.get("N_TICKETS", "5000"))
    
    # ── Step 1: TMDB Content (real API data) ──────────────────
    print(f"\n[1/8] TMDB Content ({movie_pages} movie pages + {tv_pages} TV pages)")
    df = fetch_tmdb_content(api_key, movie_pages=movie_pages, tv_pages=tv_pages)
    print(f"  Fetched {len(df)} items ({df['content_type'].value_counts().to_dict()})")

    # ── Write ─────────────────────────────────────────────────────────────────
    print("\nWriting to Databricks...")
    write_to_databricks(df, host=db_host, http_path=db_http_path, token=db_token)

    # ── Step 2: Users ─────────────────────────────────────────
    print("\n[2/8] Users")
    users = generate_users(n=n_users, content_df=df)
    users.to_csv(SEEDS_DIR / "raw_users.csv", index=False)

    # ── Step 3: Subscription Events ───────────────────────────
    print("\n[3/8] Subscription Events")
    subscriptions = generate_subscription_events(users)
    subscriptions.to_csv(SEEDS_DIR / "raw_subscription_events.csv", index=False)
    
    # ── Step 4: Watch Events ──────────────────────────────────
    print("\n[4/8] Watch Events (this may take a minute...)")
    watch_events = generate_watch_events(users, df)
    watch_events.to_csv(SEEDS_DIR / "raw_watch_events.csv", index=False)
    
    # ── Step 5: User Ratings ──────────────────────────────────
    print("\n[5/8] User Ratings")
    ratings = generate_user_ratings(watch_events)
    ratings.to_csv(SEEDS_DIR / "raw_user_ratings.csv", index=False)

    # ── Step 6: Watchlist ─────────────────────────────────────
    print("\n[6/8] Watchlists")
    watchlist = generate_watchlist(users, df, watch_events)
    watchlist.to_csv(SEEDS_DIR / "raw_user_watchlist.csv", index=False)

    # ── Step 7: Support Tickets ───────────────────────────────
    print("\n[7/8] Support Tickets")
    tickets = generate_support_tickets(users, n_tickets=n_tickets)
    tickets.to_csv(SEEDS_DIR / "raw_support_tickets.csv", index=False)
    
    # ── Summary ───────────────────────────────────────────────
    print(f"DONE! CSVs written to {SEEDS_DIR}")