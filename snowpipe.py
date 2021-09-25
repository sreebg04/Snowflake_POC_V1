from logging import getLogger
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import time
import datetime
import os
import logging

logging.basicConfig(
    filename='ingest.log',
    level=logging.DEBUG)
logger = getLogger(__name__)


# If you generated an encrypted private key, implement this method to return
# the passphrase for decrypting your private key.
def get_private_key_passphrase():
    return '<private_key_passphrase>'


with open("rsa_key.p8", 'rb') as pem_in:
    pemlines = pem_in.read()
    private_key_obj = load_pem_private_key(pemlines,
                                           "Sisi@1234".encode(),
                                           default_backend())

private_key_text = private_key_obj.private_bytes(
    Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')
# Assume the public key has been registered in Snowflake:
# private key in PEM format

# List of files in the stage specified in the pipe definition
file_list = ["demotable1_1__1.csv.gz"]
ingest_manager = SimpleIngestManager(account='lxa90481.us-east-1',
                                     host='lxa90481.us-east-1.snowflakecomputing.com',
                                     user='sree',
                                     pipe='client1.public.client1_demotable1_1_',
                                     private_key=private_key_text)
# List of files, but wrapped into a class
staged_file_list = []
for file_name in file_list:
    staged_file_list.append(StagedFile(file_name, None))

try:
    resp = ingest_manager.ingest_files(staged_file_list)
except HTTPError as e:
    # HTTP error, may need to retry
    logger.error(e)
    exit(1)

# This means Snowflake has received file and will start loading
assert (resp['responseCode'] == 'SUCCESS')

# Needs to wait for a while to get result in history
while True:
    history_resp = ingest_manager.get_history()

    if len(history_resp['files']) > 0:
        print('Ingest Report:\n')
        print(history_resp)
        break
    else:
        # wait for 20 seconds
        time.sleep(20)

    hour = timedelta(hours=1)
    date = datetime.datetime.utcnow() - hour
    history_range_resp = ingest_manager.get_history_range(date.isoformat() + 'Z')

    print('\nHistory scan report: \n')
    print(history_range_resp)
