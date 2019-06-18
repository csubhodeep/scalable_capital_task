import logging
from sqlite3 import Error

class CrudTable():

    def __init__(self,table_name,db_conn,schema):
        self.table_name = table_name
        self.conn = db_conn
        self.schema = schema # a tuple datatype
        self.__create__()

    def __execute_commands__(self,command,commit=False,many=False,rows=None):
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
        del self

    def insert(self, rows):
        # row must be a tuple
        placeholder_var = tuple(["?"]*len(rows[0]))
        cmd = "INSERT INTO " + self.table_name + " VALUES " + str(placeholder_var)
        self.__execute_commands__(cmd,commit=True,many=True,rows=rows)
        return None

    def update(self, row):

        return None

    def delete(self, row):

        return None

    def read(self,query):
        curr = self.conn.cursor()
        logging.info("executing read query")
        curr.execute(query)
        rows = curr.fetchall()

        for row in rows:
            print(row)

        return None


    def drop(self,check_if_exist=True):
        try:
            cmd = "DROP TABLE "
            if check_if_exist:
                cmd = cmd + "IF EXISTS "
            cmd = cmd + self.table_name + " ;"
            with self.conn:
                self.__execute_commands__(cmd)

            logging.info("Table {} has been dropped".format(self.table_name))
            return None
        except Error as e:
            print(e)


    def __create__(self):
        try:
            self.drop()
            cmd = "CREATE TABLE " + self.table_name + str(self.schema) +";"
            self.__execute_commands__(cmd)
            logging.info("Table {} has been created".format(self.table_name))
            return None
        except Error as e:
            print(e)


