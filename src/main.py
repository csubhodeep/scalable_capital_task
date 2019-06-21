from src.crud import CrudTable
from src.extract import Extractor
import sqlite3
from sqlite3 import Error
from json import JSONDecoder
import logging
import os
import time

class DataIngestor():

    """
    the purpose of this class is that while reading data from the disk and loading into the system memory
    it also tries to ensure that there is no duplicate records
    """

    def __init__(self,path):
        self.path=path


    def get_new_data(self,old_json_list):
        """
        this function tries to read the raw data from the text file, it is part of the Extraction step
        :param path: the path to the file
        :param old_json_list: the list of json/dict already read before
        :return: the list of json/dict rows newly added to the text file
        """
        # initialize a new empty list
        new_json_list = []
        # initialize a decoder
        dec = JSONDecoder()
        # initializing a set for fast checks if an element already exists or not (list has O(N) time complexity for this)
        check_set = set()
        # open the file and iterate through each line while
        n_lines_old = len(old_json_list)
        with open(self.path) as file:
            line_nr = 0
            for line in file:
                # increment line number
                line_nr = line_nr + 1
                # check if new lines have been added
                if line_nr>n_lines_old:
                    # decoding each line to a dictionary
                    ele = dec.decode(line)
                    # below are the checks for duplcation
                    # check if new records are not same as old
                    if ele not in old_json_list:
                        # check if new are not same as themselves
                        if str(ele) not in check_set:
                            # and appending to the total list
                            new_json_list.append(ele)
                            check_set.add(str(ele))

        logging.info("{} new rows have been fetched from disk".format(len(new_json_list)))


        return new_json_list




    def main(self):

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
                new_json_list = self.get_new_data(old_json_list)
                # instantiate the object only once - so that upon updation of new data the set of msids are not deleted
                # and the new rows can be checked for duplication especially for the dimension tables e.g. artist & release
                if flg:
                    extractor_object = Extractor(path,new_json_list)
                    flg = False

                # if new rows have been added then
                if len(new_json_list):

                    #iterate over each element in the new list and extract the necessary values for each table
                    new_data = extractor_object.get_rows_for_all_tables()

                    # the below sequence is important to ensure foreign-key constraints
                    artist_table_object.insert(new_data["artist_data"])
                    release_table_object.insert(new_data["release_data"])
                    recording_table_object.insert(new_data["recording_data"])
                    listeners_table_object.insert(new_data["listener_data"])
                    logging.info("all rows have been inserted into {}".format(db_name))

                    # add indices to the tables for faster quering
                    artist_table_object.add_index("artist_msid")
                    release_table_object.add_index("release_msid")
                    recording_table_object.add_index("recording_msid")

                else:
                    pass
                old_json_list.extend(new_json_list)
        except Error as e:
            logging.error(e)


if __name__=="__main__":
    path = os.getcwd()+"\\..\\data\\dataset.txt"
    DataIngestor(path).main()

