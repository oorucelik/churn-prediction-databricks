"""Test fetch_tmdb_content in isolation (1 page each to save API calls)."""

import os
from fetch_tmdb import fetch_tmdb_content

# Use env var so you don't hardcode the key
API_KEY = os.environ.get("TMDB_ACCESS_TOKEN")


def test_fetch_returns_dataframe():
    """Smoke test: does the function return data with the expected schema?"""
    assert API_KEY, "Set TMDB_ACCESS_TOKEN env var before running"

    # Fetch just 1 page of movies + 1 page of TV shows (fast, low API usage)
    result = fetch_tmdb_content(API_KEY, movie_pages=1, tv_pages=1)

    # Check shape — 1 page = 20 results each = ~40 rows
    assert len(result) > 0, "No content returned"
    print(f"  Got {len(result)} items")

    # Check expected columns exist
    expected_cols = [
        "content_id", "tmdb_id", "title", "content_type",
        "genres", "primary_genre", "release_date",
        "vote_average", "runtime_minutes",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"

    # Check content types
    assert set(result["content_type"].unique()) == {"movie", "tv_show"}

    # Check content_id format
    assert result["content_id"].str.startswith("TMDB_").all()
    print("  ✅ All assertions passed")
    print(result[["content_id", "title", "content_type", "runtime_minutes"]].head())


if __name__ == "__main__":
    # Run directly: python test_tmdb_fetch.py
    test_fetch_returns_dataframe()
