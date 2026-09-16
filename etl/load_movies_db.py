import json
import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

current_path = Path(__file__).resolve().parent

# 2. Navigate up to the project root, then down into the data folder
file_path = current_path.parent / "producer" / "movies.json"

with open(file_path, "r") as file:
    movies = json.load(file)

load_dotenv()
connect = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost")

connect.autocommit = True
cursor = connect.cursor()

try:
    load_movies = """
        INSERT INTO movies (movie_id, title, genre, release_year, imdb_rating)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (movie_id) DO NOTHING;
        """
    insert_rows = [
        (item.get('IMDB Id'), item.get('Title'), item.get('Genre'),
         item.get('Year'), item.get('Rating'))
        for item in movies
    ]
    cursor.executemany(load_movies, insert_rows)
    print(f"Loaded {len(insert_rows)} rows from 'movies.json' successfully!")
finally:
    cursor.close()
    connect.close()