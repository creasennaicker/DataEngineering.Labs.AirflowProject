import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import pprint
import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

DATABASE_LOCATION = "sqlite:////Users/naickercreason/dev/DataEngineering.Labs.AirflowProject/dags/my_played_tracks.sqlite"
USER_ID = 'cre'
TOKEN = "BQAgJHZyR6L3tGYXlPSvrwMhl6kd4PZj-DYWdlbTD6zsYPyo-oUJkEV1FMhQdl3FJXDa2P9TTYP05kSVOA026wDLoghNc9N2qm4ojNCEWMgqonkDAgvZQ5gSF1u2jX_3vGj2jPtVrPBgNEY9yXyoFKNP6pES3EcNuLjLNEzD"


# THIS TOKEN EXPIRES EVERY FEW MINUTES


def check_validity(df: pd.DataFrame) -> bool:
    if df.empty:
        print("There is no data here, keep moving")
        return False



if __name__ == '__main__':

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=10)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after=1638338366".format(time=yesterday_unix_timestamp),
        headers=headers)
    # INSERT WEBSITE HERE

    data = r.json()

    pprint.pprint(data)

    songs = []
    artist = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        songs.append(song["track"]["name"])
        artist.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "songs": songs,
        "artist": artist,
        "played_at": played_at_list,
        "timestamps": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["songs", "artist", "played_at_list", "timestamps"])

    # print(song_df)

    if check_validity(song_df):
        print("Valid")
    # Validates Data

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')

    cursor = conn.cursor()

    # Loading data to SQL database

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        songs VARCHAR(200),
        artist VARCHAR(200),
        played_at_list VARCHAR(200),
        timestamps VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at_list)
    )
    """
    print(sql_query)
    cursor.execute(sql_query)
    print("Database Opened")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='replace')
    except:
        print("Data already exists")

    conn.close()
    print("Database closed")
