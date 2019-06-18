import json

class Extractor():

    def __init__(self,path):
        self.filepath = path
        pass

    def __create_listener_list_from_json__(self):
        dict = json.load(open(self.filepath))
        list_of_tuples = [()]
        # do more stuffs on the dict to get lists

        return list_of_tuples

    def get_listener_rows(self):
        return self.__create_listener_list_from_json__()


    def __create_release_list_from_json__(self):
        dict = json.load(open(self.filepath))
        list_of_tuples = [()]
        # do more stuffs on the dict to get lists

        return list_of_tuples

    def get_release_rows(self):
        return self.__create_release_list_from_json__()


    def __create_recording_list_from_json__(self):
        dict = json.load(open(self.filepath))
        list_of_tuples = [()]
        # do more stuffs on the dict to get lists

        return list_of_tuples


    def get_recording_rows(self):
        return self.__create_recording_list_from_json__()



    def __create_artist_list_from_json__(self):
        dict = json.load(open(self.filepath))
        list_of_tuples = [()]
        # do more stuffs on the dict to get lists

        return list_of_tuples

    def get_artist_rows(self):
        return self.__create_artist_list_from_json__()


    def get_listener_schema(self):
        schema = "(user_name TEXT, " \
                 "recording_msid TEXT, " \
                 "listened_at INTEGER," \
                 "FOREIGN KEY(recording_msid) REFERENCES recording(recording_msid))"
        return schema

    def get_release_schema(self):
        schema = "(release_msid TEXT PRIMARY KEY, " \
                 "release_name TEXT)"
        return schema

    def get_recording_schema(self):
        schema = "(recording_msid TEXT PRIMARY KEY, " \
                 "track_name TEXT, " \
                 "release_msid TEXT , " \
                 "artist_msid TEXT ," \
                 "FOREIGN KEY(release_msid) REFERENCES release(release_msid)," \
                 "FOREIGN KEY(artist_msid) REFERENCES artist(artist_msid))"
        return schema

    def get_artist_schema(self):
        schema_tuple = "(artist_msid TEXT PRIMARY KEY," \
                       "artist_name TEXT)"
        return schema_tuple