from config import Configure
from snowflake.connector.errors import ProgrammingError
from log import logger
from snowflake_connect import Snowflake_connect
import datetime
import threading
from os import listdir
from os.path import join, isdir


class Check_copy_history:

    def __init__(self, config_file):
        self.config_file = config_file

    def load_history(self, database):
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
            sql = "select * from information_schema.load_history order by last_load_time desc;"
            cs.execute(sql)
            result = cs.fetch_pandas_all()
            res = result.loc[result['STATUS'] != "LOADED"]
            if len(res.index) > 0:
                print("Error:   ", database)
            else:
                print("Loaded successfully:   ", database)
                logger.info("Copy/Load history for database has No error")
        except ProgrammingError as db_ex:
            print(f"Programming error: {db_ex}")
            logger.error(f"Programming error: {db_ex}")
            raise
        finally:
            cs.close()
        connection.close()

    def check_history(self):
        con = Configure(self.config_file)
        config_datas = con.config()
        source = config_datas["source"]
        thread_list = []
        print("Checking loading history:  ", datetime.datetime.now())
        logger.info("Getting Copy/Load history for each database")
        for database in listdir(source):
            if isdir(join(source, database)):
                thread = threading.Thread(target=Check_copy_history.load_history, args=(self.config_file, database))
                thread_list.append(thread)
        for thr in thread_list:
            thr.start()
        for thre in thread_list:
            thre.join()
