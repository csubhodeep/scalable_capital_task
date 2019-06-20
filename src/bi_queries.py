import pandas as pd
import sqlite3

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
    cmd = "select distinct count(user_name) " \
             "from listeners " \
             "where date(listened_at,'unixepoch')='2019-03-01';"
    print(pd.read_sql_query(cmd, conn))

def query3(conn):
    cmd = "select * from listeners;"

    print(pd.read_sql_query(cmd,conn))


def make_dwh(conn):
     # execute queries to get a fact report for Task #3


    return None

if __name__ == "__main__":
    db_name = "spotify"
    conn = sqlite3.connect('{}.db'.format(db_name))
    # write specific queries to answer the questions for Task#2
    #query1(conn)
    #query2(conn)
    query3(conn)

    # Transformation step
    make_dwh(conn)