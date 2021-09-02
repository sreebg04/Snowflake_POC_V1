import os
from config import Configure
from os import listdir
from os.path import isfile, join, isdir
import shutil
import datetime
from Snowflake_upload_files import Upload_files_to_stage
from Snowflake_remove_old_staged_files import Remove_old_staged_files
from Snowflake_copy_files_to_table import Copy_files_to_table
from Snowflake_check_copy_history import Check_copy_history
from Snowflake_archive import Snowflake_archive
from log import logger


def check_for_client_files(config):
    cone = Configure(config)
    config_data = cone.config()
    source = config_data["temp"]
    temp = config_data["source"]
    clients = cone.config()
    databasetoprocess = []
    if os.path.exists(source):
        for database in listdir(source):
            if database in clients.keys() and isdir(join(source, database)) and \
                    clients[database] == listdir(join(source, database)):
                for item in clients[database]:
                    file = join(source, database, item)
                    if isfile(file):
                        databasetoprocess.append(database)
                        shutil.move(join(source, database), temp)
    return databasetoprocess


if __name__ == "__main__":
    datafiles_received = check_for_client_files("cred.json")
    if len(datafiles_received) >= 1:
        remove = Remove_old_staged_files("cred.json")
        remove.delete_old_staged_files()

        upload = Upload_files_to_stage("cred.json")
        upload.run_upload()

        copy = Copy_files_to_table("cred.json")
        copy.copy_files()

        history = Check_copy_history("cred.json")
        history.check_history()

        arch = Snowflake_archive("cred.json")
        arch.archive()
        logger.info("End of Process")
        print("end:  ", datetime.datetime.now())
    else:
        print("Waiting for file")
