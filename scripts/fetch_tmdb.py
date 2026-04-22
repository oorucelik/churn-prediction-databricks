import requests
import time
import pandas as pd

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
    Caches results to avoid hitting the API repeatedly.
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
            # get runtime from movie details
            time.sleep(0.3)  # respect rate limits (40 req / 10 sec)    
            movie_ids_resp = requests.get(
                f"{base_url}/movie/{movie['id']}",
                headers=headers,
                params={"language": "en-US"},
            )
            movie_ids_resp.raise_for_status()
            all_content[-1]["runtime_minutes"] = movie_ids_resp.json()["runtime"] if movie_ids_resp.json()["runtime"] else "N/A"
        time.sleep(0.3)  # respect rate limits (40 req / 10 sec)

        
        
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
            # get runtime from tv details
            time.sleep(0.3)  # respect rate limits (40 req / 10 sec)    
            tv_ids_resp = requests.get(
                f"{base_url}/tv/{show['id']}",
                headers=headers,
                params={"language": "en-US"},
            )
            tv_ids_resp.raise_for_status()
            all_content[-1]["runtime_minutes"] = tv_ids_resp.json()["episode_run_time"][0] if tv_ids_resp.json()["episode_run_time"] else "N/A"
        time.sleep(0.3)  # respect rate limits (40 req / 10 sec)

    return pd.DataFrame(all_content)
