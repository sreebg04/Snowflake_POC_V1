from config import Configure
from snowflake.connector.errors import ProgrammingError
from log import logger
from snowflake_connect import Snowflake_connect
import datetime
import threading
from os import listdir
from os.path import join, isdir
from splitfile import split


class Upload_files_to_stage:

    def __init__(self, config_file):
        self.config_file = config_file

    def upload(self, source_file, database):
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
            sql = "PUT file:///" + source_file + " @" + database + ";"
            cs.execute(sql)
            logger.info(sql + " executed")
        except ProgrammingError as db_ex:
            print(f"Programming error: {db_ex}")
            logger.error(f"Programming error: {db_ex}")
            raise
        finally:
            cs.close()
        connection.close()

    def run_upload(self):
        print("Split_start", datetime.datetime.now())
        con = Configure(self.config_file)
        config_datas = con.config()
        resultfiles = split(config_datas["source"])
        thread_list = []
        print("startupload:  ", datetime.datetime.now())
        logger.info(" Uploading files into Database stages")
        for file in resultfiles:
            for direc in listdir(config_datas["source"]):
                if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                    for dire in listdir(join(config_datas["source"])):
                        if dire in file:
                            thread = threading.Thread(target=Upload_files_to_stage.upload, args=(self.config_file, file, direc))
                            thread_list.append(thread)
        for thr in thread_list:
            thr.start()
        for thre in thread_list:
            thre.join()
