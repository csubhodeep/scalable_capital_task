import logging
import re

class Extractor():

    """
    sqlite3 like most RDBMS is 'schema-on-write' so this class while making the collection of values
     to be inserted into the tables also checks for consistency of the data. The following checks are done:
        1.check for sparse rows and data type of each element in a row before adding the element to the list of tuples
        2.check the msids that each one has equal length, follow the same regex pattern and are unique
            regex pattern of msids : [a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}
    if any of the above checks fail do not create the corresponding tuple for the row and skip to the next one
    Many aspects of this class are hard-coded and expect the input to be the specific ListenBrainz dataset
    """

    def __init__(self,path,list_of_jsons):
        self.filepath = path
        self.list_of_jsons = list_of_jsons
        # initialize empty sets of msids
        self.set_of_artist_msids = set([])
        self.set_of_release_msids = set([])
        self.set_of_recording_msids = set([])
        # fix the regex pattern of the msids later to be used for validation checks
        self.reg_pattern = '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
        # fix the field-names and the data-type for each table
        self.dict_of_fields_recording = {"recording_msid": str, "release_msid": str, "artist_msid": str, "track_name":str}
        self.dict_of_fields_listener = {"listened_at":int, "recording_msid":str,"user_name":str}
        self.dict_of_fields_release = {"release_msid": str, "release_name": str}
        self.dict_of_fields_artist = {"artist_msid": str, "artist_name": str}
        pass

    def __is_consistent_datatype__(self,element,dict_of_fields):
        """
        this function checks if all the values have valid datatypes
        :param element: the collection of key-value pairs in the form of dictionary
        :param dict_of_fields: a dictionary with key as the field-names and values as required data-type
        :return: flg: A flag denoting if the check passed or not
        """
        flg = True
        for (k,v) in element.items():
            if type(v)!=dict_of_fields[k]:
                flg=False
                break
        return flg

    def __is_a_valid_msid__(self,id):
        """
        this function checks the validity of an msid
        :param id: the msid
        :return: A boolean denoting whether the id passed the check or not
        """
        pattern = re.compile(self.reg_pattern)
        # check of the id is a string
        if isinstance(id,str):
            # check if the id matches the pattern
            return pattern.match(id)
        else:
            return False

    def __create_listener_list_from_json__(self):
        """
        this function accumulates the values for the listener table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a list of tuples containing the values
        """
        list_of_tuples = []
        for ele in self.list_of_jsons:
            try:
                item = {}
                item["user_name"] = ele["user_name"]
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["recording_msid"]):
                    item["recording_msid"] = ele["track_metadata"]["additional_info"]["recording_msid"]
                else:
                    logging.error("invalid msid")
                    item["recording_msid"] = 0
                item["listened_at"] = ele["listened_at"]
                if self.__is_consistent_datatype__(item,self.dict_of_fields_listener):
                    list_of_tuples.append(tuple(val for (key,val) in item.items()))
            except KeyError as error:
                logging.error("sparse data!")

        return list_of_tuples

    def __create_recording_list_from_json__(self):
        """
        this function accumulates the values for the recordings table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a list of tuples containing the values
        """
        list_of_tuples = []
        for ele in self.list_of_jsons:
            try:
                item = {}
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["recording_msid"]):
                    item["recording_msid"] = ele["track_metadata"]["additional_info"]["recording_msid"]
                else:
                    logging.error("invalid msid")
                    item["recording_msid"] = 0
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["release_msid"]):
                    item["release_msid"] = ele["track_metadata"]["additional_info"]["release_msid"]
                else:
                    logging.error("invalid msid")
                    item["release_msid"] = 0
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["artist_msid"]):
                    item["artist_msid"] = ele["track_metadata"]["additional_info"]["artist_msid"]
                else:
                    logging.error("invalid msid")
                    item["artist_msid"] = 0
                item["track_name"] = ele["track_metadata"]["track_name"]
                if self.__is_consistent_datatype__(item, self.dict_of_fields_recording):
                    # primary key contraint ensured
                    if item["recording_msid"] not in self.set_of_recording_msids:
                        self.set_of_recording_msids.add(item["recording_msid"])
                        list_of_tuples.append(tuple(val for (key, val) in item.items()))
            except KeyError as error:
                logging.error("sparse data!")


        return list_of_tuples


    def __create_release_list_from_json__(self):
        """
        this function accumulates the values for the releases table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a list of tuples containing the values
        """
        list_of_tuples = []
        for ele in self.list_of_jsons:
            try:
                item = {}
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["release_msid"]):
                    item["release_msid"] = ele["track_metadata"]["additional_info"]["release_msid"]
                else:
                    logging.error("invalid msid")
                    item["release_msid"] = 0
                item["release_name"] = ele["track_metadata"]["release_name"]
                if self.__is_consistent_datatype__(item, self.dict_of_fields_release):
                    # primary key contraint ensured
                    if item["release_msid"] not in self.set_of_release_msids:
                        self.set_of_release_msids.add(item["release_msid"])
                        list_of_tuples.append(tuple(val for (key, val) in item.items()))
            except KeyError as error:
                logging.error("sparse data!")

        return list_of_tuples

    def __create_artist_list_from_json__(self):
        """
        this function accumulates the values for the artists table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a list of tuples containing the values
        """
        list_of_tuples = []
        for ele in self.list_of_jsons:
            try:
                item = {}
                if self.__is_a_valid_msid__(ele["track_metadata"]["additional_info"]["artist_msid"]):
                    item["artist_msid"] = ele["track_metadata"]["additional_info"]["artist_msid"]
                else:
                    logging.error("invalid msid")
                    item["artist_msid"] = 0
                item["artist_name"] = ele["track_metadata"]["artist_name"]
                if self.__is_consistent_datatype__(item, self.dict_of_fields_artist):
                    # primary key contraint ensured
                    if item["artist_msid"] not in self.set_of_artist_msids:
                        self.set_of_artist_msids.add(item["artist_msid"])
                        list_of_tuples.append(tuple(val for (key, val) in item.items()))
            except KeyError as error:
                logging.error("sparse data!")

        return list_of_tuples




    # the below written schema extraction must be automatic
    # but for this purpose it has been hard-coded
    # this could be done by creating a map between the data-types of pandas and sqlite3
    # because pandas, to some extent, can infer schema automatically

    def __create_listener_schema__(self):
        schema = "(user_name TEXT, " \
                 "recording_msid TEXT, " \
                 "listened_at INTEGER," \
                 "FOREIGN KEY(recording_msid) REFERENCES recording(recording_msid))"
        return schema

    def __create_release_schema__(self):
        schema = "(release_msid TEXT PRIMARY KEY, " \
                 "release_name TEXT)"
        return schema

    def __create_recording_schema__(self):
        schema = "(recording_msid TEXT PRIMARY KEY, " \
                 "release_msid TEXT , " \
                 "artist_msid TEXT ," \
                 "track_name TEXT, " \
                 "FOREIGN KEY(release_msid) REFERENCES release(release_msid)," \
                 "FOREIGN KEY(artist_msid) REFERENCES artist(artist_msid))"
        return schema

    def __create_artist_schema__(self):
        schema_tuple = "(artist_msid TEXT PRIMARY KEY," \
                       "artist_name TEXT)"
        return schema_tuple

    def get_schema_for_all_tables(self):
        """
        public function to be accessed from the main code to obtain the schemas for all the table
        :return:
        """
        return self.__create_listener_schema__(), self.__create_release_schema__(), \
               self.__create_recording_schema__(), self.__create_artist_schema__()

    def get_rows_for_all_tables(self):
        """
        public function to be accessed from the main code to obtain the list of values to be inserted for all the table
        :return:
        """
        return self.__create_listener_list_from_json__(), self.__create_release_list_from_json__(), \
               self.__create_recording_list_from_json__(), self.__create_artist_list_from_json__()