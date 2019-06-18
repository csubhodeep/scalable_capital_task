from scan import CrudTable
import sqlite3
from sqlite3 import Error
from extract import Extractor


def load_data(path):
    # load data here
    extractor_object = Extractor(path)
    listener_rows = extractor_object.get_listener_rows()
    artist_rows = extractor_object.get_artist_rows()
    recording_rows = extractor_object.get_recording_rows()
    release_rows = extractor_object.get_release_rows()

    return listener_rows, artist_rows, recording_rows, release_rows, extractor_object

def get_schemas(extractor):
    # get schemas
    listener_schema = extractor.get_listener_schema()
    artist_schema = extractor.get_artist_schema()
    recording_schema = extractor.get_recording_schema()
    release_schema = extractor.get_release_schema()

    return listener_schema, artist_schema, recording_schema, release_schema

if __name__ == "__main__":

    path = "data/test.json"

    listener_df, artist_df, recording_df, release_df, extr = load_data(path)

    listener_schema, artist_schema, recording_schema, release_schema = get_schemas(extr)

    # define database names and table names here
    db_name = "test"
    table_name1 = "listeners"
    table_name2 = "tracks"
    table_name3 = "artists"
    table_name4 = "albums"

    # create a database and open a connection to it
    # Connects to an in-file database in the current working directory, or creates one, if it doesn't exist:
    try:
        conn = sqlite3.connect('{}.db'.format(db_name))
        listeners_table_object = CrudTable(table_name1,conn,listener_schema)
        recording_table_object = CrudTable(table_name2,conn,recording_schema)
        artist_table_object = CrudTable(table_name3,conn,artist_schema)
        release_table_object = CrudTable(table_name4,conn,release_schema)


        # populate the tables from the data loaded into memory here
        listeners_table_object.insert(listener_df)
        recording_table_object.insert(recording_df)
        artist_table_object.insert(artist_df)
        release_table_object.insert(release_df)



        # do ETL here - write specific queries to get results for Task #2
        #


    except Error as e:
        print(e)
