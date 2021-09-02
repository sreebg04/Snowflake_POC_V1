from config import Configure
from snowflake.connector.errors import ProgrammingError
from log import logger
from snowflake_connect import Snowflake_connect
import datetime
import threading
from os import listdir
from os.path import join, isdir


class Remove_old_staged_files:

    def __init__(self, config_file):
        self.config_file = config_file

    def remove_old_staged_files(self, database):
        cone = Configure(self)
        config_data = cone.config()
        conn = Snowflake_connect(self)
        connection = conn.connect()
        connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
        connection.cursor().execute("USE DATABASE " + database)
        connection.cursor().execute("USE SCHEMA " + config_data["schema"])
        connection.cursor().execute("USE ROLE " + config_data["role"])
        cs = connection.cursor()
        try:
            sql = "REMOVE @" + database + " pattern='.*.csv.gz';"
            cs.execute(sql)
            logger.info(sql + " executed")
        except ProgrammingError as db_ex:
            print(f"Programming error: {db_ex}")
            logger.error(f"Programming error: {db_ex}")
            raise
        finally:
            cs.close()
        connection.close()

    def delete_old_staged_files(self):
        con = Configure(self.config_file)
        config_datas = con.config()
        source = config_datas["temp"]
        thread_list = []
        print("remove old stage files:  ", datetime.datetime.now())
        logger.info("Deleting old staged files from database stages")
        for database in listdir(source):
            if isdir(join(source, database)):
                print(database)
                thread = threading.Thread(target=Remove_old_staged_files.remove_old_staged_files,
                                          args=(self.config_file, database))
                thread_list.append(thread)
        for thr in thread_list:
            thr.start()
        for thre in thread_list:
            thre.join()


# dat = Remove_old_staged_files("cred.json")
# dat.delete_old_staged_files()
