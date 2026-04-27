import os
import requests
import time
import pandas as pd
from databricks import sql as databricks_sql
from datetime import datetime, timezone

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
    movie_pages  = int(os.environ.get("TMDB_MOVIE_PAGES", "25"))
    tv_pages     = int(os.environ.get("TMDB_TV_PAGES", "10"))

    # ── Fetch ─────────────────────────────────────────────────────────────────
    print(f"[1/2] Fetching TMDB content ({movie_pages} movie pages, {tv_pages} TV pages)...")
    df = fetch_tmdb_content(api_key, movie_pages=movie_pages, tv_pages=tv_pages)
    print(f"  Fetched {len(df)} items ({df['content_type'].value_counts().to_dict()})")

    # ── Write ─────────────────────────────────────────────────────────────────
    print("[2/2] Writing to Databricks...")
    write_to_databricks(df, host=db_host, http_path=db_http_path, token=db_token)
