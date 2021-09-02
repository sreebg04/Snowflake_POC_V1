from config import Configure
import snowflake.connector
from log import logger
from snowflake.connector.errors import DatabaseError


class Snowflake_connect:

    def __init__(self, config_file):
        self.config_file = config_file

    def connect(self):
        connection = ""
        try:
            cone = Configure(self.config_file)
            config_data = cone.config()
            connection = snowflake.connector.connect(
                user=config_data["user"],
                password=config_data["password"],
                account=config_data["account"], )
            logger.info("Connection established successfully")
        except DatabaseError as db_ex:
            if db_ex.errno == 250001:
                print(f"Invalid username/password, please re-enter username and password...")
                logger.warning(f"Invalid username/password, please re-enter username and password...")
            else:
                raise
        except Exception as ex:
            print(f"New exception raised {ex}")
            logger.error(f"New exception raised {ex}")
            raise
        return connection


# conn = Snowflake_connect("cred.json")
# connection = conn.connect()
# print(connection)
