from config import Configure
from snowflake.connector.errors import ProgrammingError
from log import logger
from snowflake_connect import Snowflake_connect
import datetime
import threading
from os import listdir
from os.path import join, isdir
from pathlib import Path


class Copy_files_to_table:

    def __init__(self, config_file):
        self.config_file = config_file

    def copy(self, database, table):
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
            sql = """COPY into table
            FROM @stage
            file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1)
            pattern = '.*table_[1-6].csv.gz'
            on_error = 'ABORT_STATEMENT';"""
            res = sql.replace("table", table, 2)
            res_ = res.replace("stage", database, 1)
            cs.execute(res_)
            logger.info(res_ + " executed")
        except ProgrammingError as db_ex:
            print(f"Programming error: {db_ex}")
            logger.error(f"Programming error: {db_ex}")
            raise
        finally:
            cs.close()
        connection.close()

    def copy_files(self):
        con = Configure(self.config_file)
        config_datas = con.config()
        source = config_datas["source"]
        thread_list = []
        print("startcopy:  ", datetime.datetime.now())
        logger.info("Copying files into stage database")
        for database in listdir(source):
            if isdir(join(source, database)):
                for table in listdir(join(source, database)):
                    if not isdir(join(join(source, database), table)):
                        thread = threading.Thread(target=Copy_files_to_table.copy, args=(self.config_file, database, Path(table).stem))
                        thread_list.append(thread)
        for thr in thread_list:
            thr.start()
        for thre in thread_list:
            thre.join()
