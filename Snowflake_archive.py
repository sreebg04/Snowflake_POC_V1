from config import Configure
from log import logger
import shutil
from os import listdir
import os.path


class Snowflake_archive:

    def __init__(self, config_file):
        self.config_file = config_file

    def archive(self):
        con = Configure(self.config_file)
        config_datas = con.config()
        source = config_datas["source"]
        target = config_datas["archive"]
        logger.info("Moving processed files from source to archive")
        for file in listdir(source):
            shutil.move(os.path.join(source, file), target)
