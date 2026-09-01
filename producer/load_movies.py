import json

import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OMDB_API_KEY")

MOVIES = []
with open("movies.txt", "r") as file:
    for line in file:
        MOVIES.append(line.strip())

JSON_MOVIES = []
for movie in MOVIES:
    url = f"http://www.omdbapi.com/?apikey={api_key}&t={movie}"
    data = requests.get(url).json()
    if data.get("Response") == "True":
        details = {
            "IMDB Id": data["imdbID"],
            "Title": data["Title"],
            "Genre": data["Genre"],
            "Year": data["Year"],
            "Rating": data["imdbRating"]
        }
        JSON_MOVIES.append(details)
    else:
        print("Movie not found!")

with open("movies.json", "w", encoding="utf-8") as file:
    json.dump(JSON_MOVIES, file, indent=4)