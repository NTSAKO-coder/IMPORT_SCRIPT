import requests
import psycopg2

# TMDB API Key
API_KEY = "1503eda96c4c8ce850cab95a9c7b3967"

url = "https://api.themoviedb.org/3/movie/popular"
params = {"api_key": API_KEY, "page": 1}

# First request
response = requests.get(url, params=params)
response.raise_for_status()
data = response.json()

# Get total pages from response
total_pages = min(data.get("total_pages", 1), 500)  

print(f"Total pages available: {total_pages}")

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="moviesdb",
    user="postgres",
    password="Ntsako@2000"
)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        title TEXT,
        release_date TEXT,
        overview TEXT
    )
""")

for page in range(1, total_pages + 1):
    print(f"Fetching page {page}...")
    params["page"] = page
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    for movie in data.get("results", []):
        cursor.execute(
            "INSERT INTO movies (title, release_date, overview) VALUES (%s, %s, %s)",
            (movie.get("title"), movie.get("release_date"), movie.get("overview"))
        )

conn.commit()
cursor.close()
conn.close()

print(" All movies inserted successfully!")
