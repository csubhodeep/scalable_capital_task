import logging
from sqlite3 import Error

class CrudTable():

    """
    This class has been created to manage and abstract the interaction with the database from the main code
    All the necessary SQL queries for CRUD operations have been encapsulated in this class and only accessible by specific methods
    """

    def __init__(self,table_name,db_conn,schema):
        self.table_name = table_name
        self.conn = db_conn
        self.schema = schema # a tuple datatype
        self.__create__()

    def __execute_commands__(self,command,commit=False,many=False,rows=None):
        """
        this function executes the DML and DDL commands for the tables
        :param command: the command to execute
        :param commit: used in case of insertion operation - a flag to save the changes or not
        :param many: used in case of insertion operation - a flag to insert multiple rows
        :param rows: the list of tuples containing values for each row & column
        :return: None
        """
        try:
            if many and rows!=None:
                self.conn.executemany(command,rows)
            else:
                self.conn.execute(command)
            if commit:
                self.conn.commit()
        except Error as e:
            print(e)

    def dispose(self):
        """
        this function deletes the instance from the memory
        :return: None
        """
        del self

    def insert(self, rows):
        """
        this function is responsible to insert records into the table
        :param rows: the list of tuples containing the values for each row & column
        :return: None
        """
        try:
            if len(rows):
                # make a placeholder string
                placeholder_var = "("+ "?,"*(len(rows[0])-1) + "?)"
                cmd = "INSERT INTO " + self.table_name + " VALUES " + placeholder_var
                self.__execute_commands__(cmd,commit=True,many=True,rows=rows)
                return None
            else:
                pass
            logging.info("{0} rows has been inserted into {1}".format(len(rows), self.table_name))
        except Error as e:
            print(e)


    def read(self,limit=10):
        """
        this function reads and prints the first N records of the table
        :param limit: number of rows to read
        :return: None
        """
        curr = self.conn.cursor()
        logging.info("executing read query")
        cmd = "SELECT * FROM {0} LIMIT {1}".format(self.table_name,limit)
        curr.execute(cmd)
        rows = curr.fetchall()

        # print all rows
        for row in rows:
            print(row)

        return None


    def drop(self,check_if_exist=True):
        """
        this function drops the table
        :param check_if_exist: a flag to check if the table already exists
        :return: None
        """
        try:
            cmd = "DROP TABLE "
            if check_if_exist:
                cmd = cmd + "IF EXISTS "
            cmd = cmd + self.table_name + " ;"
            self.__execute_commands__(cmd)

            logging.info("Table {} has been dropped".format(self.table_name))
            return None
        except Error as e:
            print(e)

    def add_index(self,field_name):
        """
        adds the index on a particular column
        :param field_name: the column name to add the index
        :return: None
        """
        try:
            index_name = "index_{}".format(field_name)
            cmd = "CREATE INDEX {0} ON {1}({2})".format(index_name,self.table_name,field_name)
            self.__execute_commands__(cmd)
            logging.info("Table {} has been altered".format(self.table_name))
        except Error as e:
            print(e)


    def __create__(self):
        """
        this function creates a new table right after this class is instantiated
        :return: None
        """
        try:
            self.drop()
            cmd = "CREATE TABLE " + self.table_name + str(self.schema) +";"
            self.__execute_commands__(cmd)
            logging.info("Table {} has been created".format(self.table_name))
            return None
        except Error as e:
            print(e)


