import pandas as pd
import sqlite3

"""
This script basically runs a couple of queries on the database to
answer some specific BI questions. It assumes that the in-file database
is in the current working directory and is fully populated.
"""

def query1(conn):
    # first answer
    cmd = "select user_name, count(*) as cnt " \
             "from listeners " \
             "group by user_name " \
             "order by cnt desc " \
             "limit 10;"
    print(pd.read_sql_query(cmd,conn))

def query2(conn):
    # second answer
    cmd = "select distinct count(user_name) as active_user_count " \
             "from listeners " \
             "where date(listened_at,'unixepoch')='2019-03-01';"
    print(pd.read_sql_query(cmd, conn))

def query3(conn):
    cmd = "select user_name, min(listened_at) as first_listened_at, track_name " \
          "from listeners join recordings on listeners.recording_msid=recordings.recording_msid " \
          "group by user_name " \
          "limit 10;"

    print(pd.read_sql_query(cmd,conn))


def make_dwh(conn):
    # Transformation step
    # execute queries to get a fact report for Task #3

    # 1st metric - number of distinct users
    cmd1 = "select distinct count(user_name) from listeners"
    print(pd.read_sql_query(cmd1,conn))

    # 2nd metric - 10 most popular tracks
    cmd2 = "select track_name, count(*) as times_listened " \
           "from listeners join recordings " \
           "on listeners.recording_msid=recordings.recording_msid " \
           "group by track_name " \
           "order by times_listened desc " \
           "limit 10;"
    print(pd.read_sql_query(cmd2,conn))

    # 3rd metric - 5 most popular artists
    cmd3 = "select artist_name, count(*) as times_listened " \
           "from listeners join (select artist_name, recording_msid " \
           "from artists join recordings " \
           "on artists.artist_msid=recordings.artist_msid) as t " \
           "on listeners.recording_msid=t.recording_msid " \
           "group by artist_name " \
           "order by times_listened desc " \
           "limit 5;"

    print(pd.read_sql_query(cmd3,conn))

    """
    some other metrics that might be useful are:
    1. listening duration for each track can give us who is the most engaged user
    2. ratings/reviews for each track can give us the most liked track
    3. genre of music can help us understand the taste of the user-base.
    """

    return None

if __name__ == "__main__":
    db_name = "spotify"
    conn = sqlite3.connect('{}.db'.format(db_name))
    # write specific queries to answer the questions for Task#2
    query1(conn)
    query2(conn)
    query3(conn)
    make_dwh(conn)