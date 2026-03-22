import os
from pathlib import Path

from dotenv import load_dotenv
from databricks import sql

load_dotenv(Path(__file__).resolve().parent / ".env")

DDL = """
DROP TABLE IF EXISTS workspace.default.raw_google_fit;

CREATE TABLE workspace.default.raw_google_fit
USING PARQUET
LOCATION 's3://zoomcamp--497675597195-eu-west-2-an/raw/googlefit/hamon_googlefit_medical_realistic.parquet';
"""

def main() -> None:
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )

    try:
        with conn.cursor() as cursor:
            for statement in [s.strip() for s in DDL.split(";") if s.strip()]:
                cursor.execute(statement)
    finally:
        conn.close()

    print("Databricks raw table refreshed successfully.")

if __name__ == "__main__":
    main()