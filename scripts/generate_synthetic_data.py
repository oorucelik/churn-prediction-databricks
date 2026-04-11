# scripts/generate_synthetic_data.py
# =============================================================================
# Generates realistic churn-scenario data for the portfolio project.
#
# Data sources:
#   - TMDB API (real movies & TV shows) → content catalog
#   - Faker (synthetic) → users, subscriptions, support tickets
#   - Synthetic watch events mapped to real TMDB content
#
# Output: CSV files in seeds/ for dbt to load.
#
# ML use cases this data enables:
#   1. Churn prediction   — engagement decay, subscription downgrades
#   2. Content recommendation — collaborative filtering, genre preferences
#   3. Engagement scoring — binge detection, watchlist conversion
# =============================================================================

import os
import json
import time
import requests
from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
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
SEEDS_DIR = Path(__file__).parent.parent / "seeds"


# ─── TMDB API: FETCH REAL CONTENT CATALOG ────────────────────────────────────

def fetch_tmdb_content(api_key: str, movie_pages: int = 25, tv_pages: int = 10):
    """
    Fetch popular movies and TV shows from TMDB API.
    Returns a DataFrame with unified content catalog.
    Caches results to avoid hitting the API repeatedly.
    """
    cache_file = CACHE_DIR / "tmdb_content.json"

    if cache_file.exists():
        print(f"  Loading cached TMDB data from {cache_file}")
        with open(cache_file, "r", encoding="utf-8") as f:
            content = json.load(f)
        return pd.DataFrame(content)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    base_url = "https://api.themoviedb.org/3"
    headers = {"Authorization": f"Bearer {api_key}"}
    all_content = []

    # Fetch popular movies
    print(f"  Fetching {movie_pages} pages of popular movies from TMDB...")
    for page in range(1, movie_pages + 1):
        resp = requests.get(
            f"{base_url}/movie/popular",
            headers=headers,
            params={"page": page, "language": "en-US"},
        )
        resp.raise_for_status()
        for movie in resp.json()["results"]:
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
                "runtime_minutes": random.randint(80, 180),  # TMDB list endpoint doesn't include runtime
            })
        time.sleep(0.3)  # respect rate limits (40 req / 10 sec)

    # Fetch popular TV shows
    print(f"  Fetching {tv_pages} pages of popular TV shows from TMDB...")
    for page in range(1, tv_pages + 1):
        resp = requests.get(
            f"{base_url}/tv/popular",
            headers=headers,
            params={"page": page, "language": "en-US"},
        )
        resp.raise_for_status()
        for show in resp.json()["results"]:
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
                "runtime_minutes": random.randint(25, 60),  # per episode 
            })
        time.sleep(0.3)

    # Cache to disk
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(all_content, f, ensure_ascii=False, indent=2)
    print(f"  Cached {len(all_content)} content items to {cache_file}")

    return pd.DataFrame(all_content)


def generate_fallback_content(n_movies: int = 500, n_shows: int = 200):
    """
    Generate synthetic content catalog when TMDB API key is not available.
    Uses realistic genre distributions and naming patterns.
    """
    print("  No TMDB API key found — generating synthetic content catalog")
    genres = ["Action", "Comedy", "Drama", "Thriller", "Sci-Fi",
              "Horror", "Romance", "Documentary", "Animation", "Fantasy",
              "Crime", "Mystery", "Adventure", "Family", "Music"]
    content = []

    for i in range(n_movies):
        g = random.sample(genres, k=random.randint(1, 3))
        content.append({
            "content_id": f"MOV_{i:05d}",
            "tmdb_id": 100_000 + i,
            "title": f"{fake.catch_phrase()} {random.choice(['', 'II', 'III', 'Returns', 'Rising'])}".strip(),
            "content_type": "movie",
            "genres": "|".join(g),
            "primary_genre": g[0],
            "release_date": fake.date_between(start_date="-10y", end_date="today").isoformat(),
            "vote_average": round(random.uniform(4.0, 9.0), 1),
            "vote_count": random.randint(50, 20_000),
            "popularity": round(random.uniform(5, 500), 2),
            "original_language": random.choices(["en", "es", "fr", "ko", "ja"], weights=[70, 8, 5, 10, 7])[0],
            "overview": fake.paragraph(nb_sentences=2),
            "runtime_minutes": random.randint(80, 180),
        })

    for i in range(n_shows):
        g = random.sample(genres, k=random.randint(1, 3))
        content.append({
            "content_id": f"TVS_{i:05d}",
            "tmdb_id": 200_000 + i,
            "title": f"The {fake.word().title()} {random.choice(['Chronicles', 'Files', 'Diaries', 'Effect', 'Legacy', 'Show'])}",
            "content_type": "tv_show",
            "genres": "|".join(g),
            "primary_genre": g[0],
            "release_date": fake.date_between(start_date="-8y", end_date="today").isoformat(),
            "vote_average": round(random.uniform(5.0, 9.2), 1),
            "vote_count": random.randint(100, 15_000),
            "popularity": round(random.uniform(10, 600), 2),
            "original_language": random.choices(["en", "es", "ko", "ja", "de"], weights=[65, 10, 12, 8, 5])[0],
            "overview": fake.paragraph(nb_sentences=2),
            "runtime_minutes": random.randint(25, 60),  # per episode
        })

    return pd.DataFrame(content)


# ─── USER GENERATION ─────────────────────────────────────────────────────────

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


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Content Catalog (TMDB or fallback) ────────────
    api_key = os.environ.get("TMDB_API_KEY") or os.environ.get("TMDB_ACCESS_TOKEN")
    print("=" * 60)
    print("CHURN PREDICTION PIPELINE — Synthetic Data Generator")
    print("=" * 60)

    print("\n[1/7] Content Catalog")
    if api_key:
        content = fetch_tmdb_content(api_key, movie_pages=25, tv_pages=10)
    else:
        print("  TIP: Set TMDB_API_KEY env var to fetch real data from TMDB API")
        print("       Sign up free at https://developer.themoviedb.org")
        content = generate_fallback_content(n_movies=500, n_shows=200)

    content.to_csv(SEEDS_DIR / "raw_content_catalog.csv", index=False)
    print(f"  [OK] {len(content)} content items ({content['content_type'].value_counts().to_dict()})")

    # ── Step 2: Users ─────────────────────────────────────────
    print("\n[2/7] Users")
    # 1K users for local dev (dbt seed). Scale to 10K+ when loading via Airflow.
    users = generate_users(n=1_000, content_df=content)
    users.to_csv(SEEDS_DIR / "raw_users.csv", index=False)
    churned = users["is_churned"].sum()
    print(f"  [OK] {len(users)} users ({churned} churned, {len(users) - churned} active)")

    # ── Step 3: Subscription Events ───────────────────────────
    print("\n[3/7] Subscription Events")
    subscriptions = generate_subscription_events(users)
    subscriptions.to_csv(SEEDS_DIR / "raw_subscription_events.csv", index=False)
    print(f"  [OK] {len(subscriptions)} events ({subscriptions['event_type'].value_counts().to_dict()})")

    # ── Step 4: Watch Events ──────────────────────────────────
    print("\n[4/7] Watch Events (this may take a minute...)")
    watch_events = generate_watch_events(users, content)
    watch_events.to_csv(SEEDS_DIR / "raw_watch_events.csv", index=False)
    print(f"  [OK] {len(watch_events)} watch events")
    print(f"    Unique user-content pairs: {watch_events.groupby(['customer_id', 'content_id']).ngroups}")

    # ── Step 5: User Ratings ──────────────────────────────────
    print("\n[5/7] User Ratings")
    ratings = generate_user_ratings(watch_events)
    ratings.to_csv(SEEDS_DIR / "raw_user_ratings.csv", index=False)
    print(f"  [OK] {len(ratings)} ratings (avg: {ratings['rating'].mean():.1f})")

    # ── Step 6: Watchlist ─────────────────────────────────────
    print("\n[6/7] Watchlists")
    watchlist = generate_watchlist(users, content, watch_events)
    watchlist.to_csv(SEEDS_DIR / "raw_user_watchlist.csv", index=False)
    conversion = watchlist["is_watched"].mean() * 100
    print(f"  [OK] {len(watchlist)} watchlist items ({conversion:.0f}% conversion rate)")

    # ── Step 7: Support Tickets ───────────────────────────────
    print("\n[7/7] Support Tickets")
    tickets = generate_support_tickets(users, n_tickets=5_000)
    tickets.to_csv(SEEDS_DIR / "raw_support_tickets.csv", index=False)
    print(f"  [OK] {len(tickets)} tickets")

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("DONE! Files written to seeds/")
    print("=" * 60)
    print(f"""
  raw_content_catalog.csv      {len(content):>8,} rows
  raw_users.csv                {len(users):>8,} rows
  raw_subscription_events.csv  {len(subscriptions):>8,} rows
  raw_watch_events.csv         {len(watch_events):>8,} rows
  raw_user_ratings.csv         {len(ratings):>8,} rows
  raw_user_watchlist.csv       {len(watchlist):>8,} rows
  raw_support_tickets.csv      {len(tickets):>8,} rows

  Next steps:
    1. cd churn-prediction-pipeline
    2. dbt deps
    3. dbt seed
    4. dbt run
    5. dbt test
""")
