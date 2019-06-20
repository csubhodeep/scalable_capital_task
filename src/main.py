from src.crud import CrudTable
import sqlite3
from sqlite3 import Error
from src.extract import Extractor
from json import JSONDecoder
import logging
import os
import time


def get_new_data(path,old_json_list):
    """
    this function tries to read the data from the text file, it is part of the Extraction step
    :param path: the path to the file
    :param old_json_list: the list of json/dict already read before
    :return: the list of json/dict rows newly added to the text file
    """
    # initialize a new empty list
    new_json_list = []
    # initialize a new list which is supposed to contain all the rows
    total_json_list = []
    # initialize a decoder
    dec = JSONDecoder()
    # open the file and iterate through each line while decoding each line to a dictionary and appending to the total list
    with open(path) as file:
        for line in file:
            total_json_list.append(dec.decode(line))

    # add only new rows to the new list
    for ele in total_json_list:
        if ele not in old_json_list and ele not in new_json_list:
            new_json_list.append(ele)

    return new_json_list




if __name__ == "__main__":

    path = os.getcwd()+"\\..\\data\\test2.json"


    # making a dummy extractor object for defining schema that is later used for creation of the tables
    extr = Extractor(path="",list_of_jsons=[])
    listener_schema, release_schema, recording_schema, artist_schema = extr.get_schema_for_all_tables()

    # define database names and table names here
    db_name = "spotify"
    table_name1 = "listeners"
    table_name2 = "recordings"
    table_name3 = "artists"
    table_name4 = "releases"

    # create a database and open a connection to it
    try:
        # Connects to an in-file database in the current working directory, or creates one, if it doesn't exist:
        conn = sqlite3.connect('{}.db'.format(db_name))

        # create table objects
        listeners_table_object = CrudTable(table_name1,conn,listener_schema)
        recording_table_object = CrudTable(table_name2,conn,recording_schema)
        artist_table_object = CrudTable(table_name3,conn,artist_schema)
        release_table_object = CrudTable(table_name4,conn,release_schema)


        # initially it is assumed that data is not updated
        old_json_list = []

        # a flg is used to instantiate the Extractor object only once required to load the values
        flg = True
        #while True:
        if True:
            # setting a refresh rate of 1s
            time.sleep(1)
            # check for new data
            new_json_list = get_new_data(path,old_json_list)
            # instantiate the object only once - so that upon updation of new data the set of msids are not deleted
            # and the new rows can be checked for duplication especially for the dimension tables e.g. artist & release
            if flg:
                extractor_object = Extractor(path,new_json_list)
                flg = False

            # if new rows have been added then
            if len(new_json_list):
                #iterate over each element in the new list and extract the necessary values for each table
                new_listener_data, new_release_data, new_recording_data, new_artist_data = extractor_object.get_rows_for_all_tables()

                # the below sequence is important to ensure foreign-key constraints
                artist_table_object.insert(new_artist_data)
                release_table_object.insert(new_release_data)
                listeners_table_object.insert(new_listener_data)
                recording_table_object.insert(new_recording_data)
            else:
                pass
            old_json_list.extend(new_json_list)
    except Error as e:
        logging.error(e)
