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
        self.reg_pattern = re.compile("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")
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
        # check if the id is a string
        if isinstance(id,str):
            # check if the id matches the pattern
            if self.reg_pattern.match(id):
                return True
            else:
                return False
        else:
            return False

    def __extract_artist_data__(self,dict):
        """
        this function extracts the values for a row in the artist table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a tuple containing the values
        """
        art = {}
        try:
            if self.__is_a_valid_msid__(dict["track_metadata"]["additional_info"]["artist_msid"]):
                art["artist_msid"] = dict["track_metadata"]["additional_info"]["artist_msid"]
            else:
                logging.error(
                    "invalid artist msid: " + str(dict["track_metadata"]["additional_info"]["artist_msid"]))
                art["artist_msid"] = 0
            art["artist_name"] = dict["track_metadata"]["artist_name"]
            if self.__is_consistent_datatype__(art, self.dict_of_fields_artist):
                # primary key contraint ensured
                if art["artist_msid"] not in self.set_of_artist_msids:
                    self.set_of_artist_msids.add(art["artist_msid"])
                    return tuple(val for (key, val) in art.items())
                else:
                    return None
            else:
                return None
        except KeyError as error:
            logging.error("sparse artist data")
            return None

    def __extract_release_data__(self,dict):
        """
        this function extracts the values for a row in the release table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a tuple containing the values
        """
        release = {}
        try:
            if self.__is_a_valid_msid__(dict["track_metadata"]["additional_info"]["release_msid"]):
                release["release_msid"] = dict["track_metadata"]["additional_info"]["release_msid"]
            else:
                logging.error(
                    "invalid artist msid: " + str(dict["track_metadata"]["additional_info"]["release_msid"]))
                release["release_msid"] = 0
            release["release_name"] = dict["track_metadata"]["release_name"]
            if self.__is_consistent_datatype__(release, self.dict_of_fields_release):
                # primary key contraint ensured
                if release["release_msid"] not in self.set_of_release_msids:
                    self.set_of_release_msids.add(release["release_msid"])
                    return tuple(val for (key, val) in release.items())
                else:
                    return None
            else:
                return None
        except KeyError as error:
            logging.error("sparse release data")
            return None

    def __extract_recording_data__(self,dict):
        """
        this function extracts the values for a row in the recordings table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a tuple containing the values
        """
        recording = {}
        try:
            if self.__is_a_valid_msid__(dict["track_metadata"]["additional_info"]["recording_msid"]):
                recording["recording_msid"] = dict["track_metadata"]["additional_info"]["recording_msid"]
            else:
                logging.error(
                    "invalid recording msid: " + str(dict["track_metadata"]["additional_info"]["recording_msid"]))
                recording["recording_msid"] = 0

            # check if key exists in parent table - releases
            if dict["track_metadata"]["additional_info"]["release_msid"] in self.set_of_release_msids:
                recording["release_msid"] = dict["track_metadata"]["additional_info"]["release_msid"]
            else:
                logging.error("invalid release msid: " + str(dict["track_metadata"]["additional_info"]["release_msid"]))
                recording["release_msid"] = 0

            # check if key exists in parent table - artists
            if dict["track_metadata"]["additional_info"]["artist_msid"] in self.set_of_artist_msids:
                recording["artist_msid"] = dict["track_metadata"]["additional_info"]["artist_msid"]
            else:
                logging.error("invalid artist msid: " + str(dict["track_metadata"]["additional_info"]["artist_msid"]))
                recording["artist_msid"] = 0

            recording["track_name"] = dict["track_metadata"]["track_name"]

            if self.__is_consistent_datatype__(recording, self.dict_of_fields_recording):
                # primary key contraint ensured
                if recording["recording_msid"] not in self.set_of_recording_msids:
                    self.set_of_recording_msids.add(recording["recording_msid"])
                    return tuple(val for (key, val) in recording.items())
                else:
                    return None
            else:
                return None
        except KeyError as error:
            logging.error("sparse recording data!")
            return None

    def __extract_listener_data__(self,dict):
        """
        this function extracts the values for a row in the listener table - also ensuring that each value
        qualifies all the consistency checks.
        :return: a tuple containing the values
        """
        listener = {}
        try:
            listener["user_name"] = dict["user_name"]
            # check if key exists in parent table
            if dict["track_metadata"]["additional_info"]["recording_msid"] in self.set_of_recording_msids:
                listener["recording_msid"] = dict["track_metadata"]["additional_info"]["recording_msid"]
            else:
                logging.error(
                    "invalid recording msid: " + str(dict["track_metadata"]["additional_info"]["recording_msid"]))
                listener["recording_msid"] = 0
            listener["listened_at"] = dict["listened_at"]
            if self.__is_consistent_datatype__(listener, self.dict_of_fields_listener):
                return tuple(val for (key, val) in listener.items())
            else:
                return None
        except KeyError as error:
            logging.error("sparse listener data!")
            return None

    def __create_list_of_tuples_from_json__(self):
        """
        extract the data corresponding to each table
        :return: dictionary containing the keys as table names and list of tuples as values
        """
        rows = {}
        rows["artist_data"] = []
        rows["release_data"] = []
        rows["recording_data"] = []
        rows["listener_data"] = []
        for ele in self.list_of_jsons:
            artist_data_row = self.__extract_artist_data__(ele)
            if artist_data_row:
                rows["artist_data"].append(artist_data_row)
            release_data_row = self.__extract_release_data__(ele)
            if release_data_row:
                rows["release_data"].append(release_data_row)
            recording_data_row = self.__extract_recording_data__(ele)
            if recording_data_row:
                rows["recording_data"].append(recording_data_row)

            listener_data_row = self.__extract_listener_data__(ele)
            if listener_data_row:
                rows["listener_data"].append(listener_data_row)

        return rows

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

    # the public functions although python doesnot support explicit access modifiers
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
        :return: dictionary of list of tuples
        """
        return self.__create_list_of_tuples_from_json__()