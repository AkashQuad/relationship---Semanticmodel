import os
import re
import time
import logging
import tempfile
import requests
import pandas as pd

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from extractor import extract_metadata_from_twbx


from pydantic import BaseModel
from typing import List




# ============================================================
# ENV + CONFIG
# ============================================================

load_dotenv()

POWERBI_API = "https://api.powerbi.com/v1.0/myorg"

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

TWBX_CONTAINER = os.getenv("TWBX_CONTAINER")
CSV_CONTAINER = os.getenv("CSV_CONTAINER")

REPORT_NAME = "Final_Sales_Report"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tableau-pbi-migrator")

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(title="Tableau → Power BI Semantic Builder")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)



class Relationship(BaseModel):
    fromTable: str
    fromColumn: str
    toTable: str
    toColumn: str
    relationshipType: str


class SemanticModelRequest(BaseModel):
    folder_name: str
    target_workspace_id: str
    relationships: List[Relationship]



# ============================================================
# HELPERS
# ============================================================




def extract_second_word_table_name(filename: str) -> str:
    base = filename.split(".csv")[0]
    parts = base.split("_")
    table_name = parts[1] if len(parts) >= 2 else parts[0]
    return re.sub(r"[^a-zA-Z]", "", table_name).lower()


def get_auth_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
    )

    resp.raise_for_status()
    return resp.json()["access_token"]


def download_twbx_from_blob(folder_name: str) -> str:
    blob_service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container = blob_service.get_container_client(TWBX_CONTAINER)

    twbx_blob_name = f"{folder_name}.twbx"

    data = container.download_blob(twbx_blob_name).readall()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".twbx")
    tmp.write(data)
    tmp.close()

    return tmp.name

# ============================================================
# ENDPOINT 1 — METADATA ONLY
# ============================================================

@app.post("/extract-metadata")
def extract_metadata(folder_name: str):
    try:
        twbx_path = download_twbx_from_blob(folder_name)
        metadata = extract_metadata_from_twbx(twbx_path)
        os.remove(twbx_path)

        return {
            "status": "SUCCESS",
            "metadata": metadata
        }

    except Exception as e:
        log.exception("Metadata extraction failed")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINT 2 — CREATE SEMANTIC MODEL
# ============================================================
@app.post("/create-semantic-model")
def create_semantic_model(request: SemanticModelRequest):
    try:
        folder_name = request.folder_name
        target_workspace_id = request.target_workspace_id
        relationships = request.relationships

        token = get_auth_token()

        blob_service = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        container = blob_service.get_container_client(CSV_CONTAINER)

        blob_tables = {}
        prefix = f"{folder_name.rstrip('/')}/"

        for blob in container.list_blobs(name_starts_with=prefix):
            filename = os.path.basename(blob.name)

            if not filename.lower().endswith(".csv"):
                continue

            table_name = extract_second_word_table_name(filename)

            data = container.download_blob(blob.name).readall()
            blob_tables[table_name] = pd.read_csv(
                pd.io.common.BytesIO(data)
            )

        if not blob_tables:
            raise Exception("No CSV tables found")

        pbi_relationships = []
        for r in relationships:
            pbi_relationships.append({
                "name": f"{r.fromTable}_{r.toTable}",
                "fromTable": r.fromTable,
                "fromColumn": r.fromColumn,
                "toTable": r.toTable,
                "toColumn": r.toColumn,
                "crossFilteringBehavior": "BothDirections",
            })

        dataset_payload = {
            "name": f"{REPORT_NAME}_DS",
            "tables": [],
            "relationships": pbi_relationships,
        }

        for table_name, df in blob_tables.items():
            columns = []

            for col in df.columns:
                if "id" in col.lower():
                    dtype = "Int64"
                elif df[col].dtype == "float64":
                    dtype = "Double"
                elif df[col].dtype == "int64":
                    dtype = "Int64"
                else:
                    dtype = "String"

                columns.append({
                    "name": col,
                    "dataType": dtype,
                })

            dataset_payload["tables"].append({
                "name": table_name,
                "columns": columns,
            })

        ds_resp = requests.post(
            f"{POWERBI_API}/groups/{target_workspace_id}/datasets",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json=dataset_payload,
        )

        ds_resp.raise_for_status()

        dataset_id = ds_resp.json()["id"]

        time.sleep(5)

        for table_name, df in blob_tables.items():
            rows = df.where(pd.notnull(df), None).to_dict(orient="records")

            for i in range(0, len(rows), 2500):
                requests.post(
                    f"{POWERBI_API}/groups/{target_workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    },
                    json={"rows": rows[i:i + 2500]},
                ).raise_for_status()

        return {
            "status": "SUCCESS",
            "dataset_id": dataset_id,
            "relationships_created": len(pbi_relationships),
        }

    except Exception as e:
        log.exception("Semantic model creation failed")
        raise HTTPException(status_code=500, detail=str(e))
