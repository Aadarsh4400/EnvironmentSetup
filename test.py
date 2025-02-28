import requests
import snowflake.connector
import os, logging
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.WARN)

def connect_snow():
    private_key = f"-----BEGIN PRIVATE KEY-----\n{os.getenv('PRIVATE_KEY')}\n-----END PRIVATE KEY-----"
    p_key = serialization.load_pem_private_key(
        private_key.encode(),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'},
    )

# GitHub CSV URL (RAW) fetches the fle from that path 
GITHUB_CSV_URL = "https://raw.githubusercontent.com/pallavi123-star/SnowflakeTASK/main/category.csv"


# Download CSV define the file path where we want to store 
csv_path = "/tmp/data.csv"
#send an http request to downlod the file
response = requests.get(GITHUB_CSV_URL)
if response.status_code == 200:
    with open(csv_path, "wb") as g:
        f.write(response.content)
        #save the csv file
    print("CSV downloaded successfully.")
else:
    print("Failed to download CSV.")
    exit()

# Snowflake Connection
conn = connect_snow()


# Upload CSV to Stage
conn.cursor().execute(f"PUT file://{csv_path} @%LIST_CATEGORY")

print(f"File uploaded to stage ",csv_path)

pipe_name='LIFT_CATEGORY_PIPE'

# Trigger Snowpipe
conn.cursor().execute(f"ALTER PIPE {pipe_name} REFRESH")
print(f"Snowpipe {pipe_name} triggered.")

# Close connection
conn.close()
print("✅ CSV Loaded into Snowflake via Snowpipe!")
