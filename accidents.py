from flask import Flask, jsonify
from sodapy import Socrata
import pandas as pd

app = Flask(__name__)

# Inicialize o cliente Socrata
client = Socrata("data.nashville.gov", None)

@app.route('/accidents', methods=['GET'])
def get_accidents():
    # Obtenha resultados do dataset
    results = client.get("6v6w-hpcw", limit=20)
    # Converta os resultados em um DataFrame pandas
    results_df = pd.DataFrame.from_records(results)
    # Converta o DataFrame em JSON
    results_json = results_df.to_json(orient="records")
    return results_json

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
