import unittest
from src.extract import Extractor
from json import JSONDecoder

class TestExtractorMethods(unittest.TestCase):

    def setUp(self):
        """
        this method sets up the elements used for the test
        :return:
        """
        test_json_list = []
        dec = JSONDecoder()
        with open("test_data\\test.txt") as file:
            for line in file:
                test_json_list.append(dec.decode(line))
        self.test_object = Extractor(test_json_list)

    def test_get_rows_for_all_tables(self):
        """
        this function tests if all the data read from the file were properly processed ensuring consistency
        :return: None
        """
        row = self.test_object.get_rows_for_all_tables()
        assert(len(row["listener_data"])==7)
        assert(len(row["artist_data"])==4)
        assert(len(row["release_data"])==5)



    def tearDown(self):
        """
        this function removes the test object from the memory
        :return:
        """
        self.test_object.dispose()


if __name__ == '__main__':
    unittest.main()