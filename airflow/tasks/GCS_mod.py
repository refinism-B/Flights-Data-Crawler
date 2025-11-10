import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


"""
這個模組主要用於GCS的操作。
通常寫作 import GCS_mod as gcs。
"""


def L_upload_to_gcs(gcs_setting: dict):
    """
    提供"gcs_setting"上傳設定檔，會自動抓取"bucket_name"、"destination"、
    "source_file_name"等資訊，並自動將檔案上傳至GCS。

    注意：請準備好GCS的json key檔案，並將路徑寫入.env。
    """

    load_dotenv()

    credential_path = os.getenv("GCS_KEY_PATH")
    credentials = service_account.Credentials.from_service_account_file(
        credential_path)

    bucket_name = gcs_setting["bucket_name"]
    destination = gcs_setting["destination"]
    source_file_name = gcs_setting["source_file_name"]

    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(source_file_name)

    print(f"{bucket_name}/{destination}上傳成功！")
