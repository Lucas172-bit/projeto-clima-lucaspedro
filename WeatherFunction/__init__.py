import datetime
import json
import logging
import os

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().isoformat()
    logging.info(f"Function executada em {utc_timestamp}")

    # 1. Lê variáveis de ambiente
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    city = os.environ.get("CITY", "Sao Paulo,BR")
    storage_conn_str = os.environ.get("AzureWebJobsStorage")

    if not api_key:
        logging.error("OPENWEATHER_API_KEY não configurado")
        return

    if not storage_conn_str:
        logging.error("AzureWebJobsStorage não configurado")
        return

    # 2. Chama a API do OpenWeather
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q={city}&appid={api_key}&units=metric&lang=pt_br"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Erro ao chamar OpenWeather: {e}")
        return

    dados = resp.json()

    # 3. Conecta no Blob Storage usando a connection string padrão
    blob_service = BlobServiceClient.from_connection_string(storage_conn_str)

    container_name = "clima"
    folder = city.replace(",", "_").replace(" ", "_")
    blob_name = f"{folder}/{utc_timestamp}.json"

    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

    blob_client.upload_blob(json.dumps(dados), overwrite=True)

    logging.info(f"Dados salvos em {container_name}/{blob_name}")
