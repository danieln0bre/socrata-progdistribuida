from flask import Flask, jsonify, request
from sodapy import Socrata
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

# Inicialize o cliente Socrata
client = Socrata("data.nashville.gov", None)

def get_last_24_hours_data(dataset_id, client):
    # Defina o timestamp para 24 horas atrás
    last_24_hours = datetime.now() - timedelta(hours=24)
    last_24_hours_str = last_24_hours.strftime("%Y-%m-%dT%H:%M:%S.%f")

    # Query para obter dados das últimas 24 horas
    query = f"date_time >= '{last_24_hours_str}'"
    results = client.get(dataset_id, where=query, limit=20)

    return results

class DataAdapter:
    @staticmethod
    def format_data(data):
        # Converte os dados em um DataFrame pandas
        results_df = pd.DataFrame.from_records(data)
        # Converte o DataFrame em JSON
        results_json = results_df.to_json(orient="records")
        return results_json

@app.route('/accidents', methods=['GET'])
def get_accidents():
    # Obtenha resultados do dataset das últimas 24 horas
    results = get_last_24_hours_data("6v6w-hpcw", client)
    # Formate os dados usando o adapter
    formatted_data = DataAdapter.format_data(results)
    return formatted_data

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
