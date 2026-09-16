import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
connect = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost")

connect.autocommit = True
cursor = connect.cursor()
try:
    create_table_movies = """
        CREATE TABLE IF NOT EXISTS movies (
            movie_id VARCHAR(20) PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            genre VARCHAR(50) NOT NULL,
            release_year int,
            imdb_rating NUMERIC(3, 1)
        );
        """
    cursor.execute(create_table_movies)
    print("Table 'Movies' created successfully!")

    create_table_movie_events = """
        CREATE TABLE IF NOT EXISTS movie_events (
            event_id VARCHAR(20) PRIMARY KEY,
            movie_id VARCHAR(20) NOT NULL REFERENCES movies (movie_id),
            user_id VARCHAR(20) NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );"""
    cursor.execute(create_table_movie_events)
    print("Table 'Movie_Events' created successfully!")

    create_table_movie_metrics = """
        CREATE TABLE IF NOT EXISTS movie_metrics (
            movie_id VARCHAR(20) PRIMARY KEY REFERENCES movies (movie_id),
            views INT NOT NULL DEFAULT 0,
            views_last_5_min INT NOT NULL DEFAULT 0,
            avg_rating FLOAT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );"""
    cursor.execute(create_table_movie_metrics)
    print("Table 'Movie_Metrics' created successfully!")

except psycopg2.errors.DuplicateTable:
    print("Table already exists!")
finally:
    cursor.close()
    connect.close()