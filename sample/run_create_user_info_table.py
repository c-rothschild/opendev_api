import os
from dotenv import load_dotenv
from opendev_api.client import OpenDevData

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")



client = OpenDevData("./data", "odd.duckdb")


client.conn.execute("""
        DROP TABLE IF EXISTS user_info
    """)
client.create_user_info_table(GITHUB_TOKEN)
client.close()

