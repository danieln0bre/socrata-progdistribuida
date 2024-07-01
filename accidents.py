from flask import Flask, jsonify, request, send_file
import pandas as pd
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import io

app = Flask(__name__)

# Conecte-se ao MongoDB
mongo_uri = "mongodb+srv://admin:admin@socrata.y2z5nfp.mongodb.net/Socrata"
mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))

try:
    mongo_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Função para obter dados das últimas 24 horas do MongoDB
def get_last_24_hours_data_from_mongo(collection_name):
    db = mongo_client.Socrata
    collection = db[collection_name]
    last_24_hours = datetime.now() - timedelta(hours=24)
    results = collection.find({"date_time": {"$gte": last_24_hours}})
    return list(results)

class DataAdapter:
    @staticmethod
    def format_data_to_csv(data):
        df = pd.DataFrame(data)
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return output

@app.route('/accidents', methods=['GET'])
def get_accidents():
    # Obtenha resultados do MongoDB das últimas 24 horas
    results = get_last_24_hours_data_from_mongo("accidents_prf_collection")
    # Formate os dados em CSV usando o adapter
    csv_data = DataAdapter.format_data_to_csv(results)
    return send_file(io.BytesIO(csv_data.getvalue().encode()), mimetype='text/csv', as_attachment=True, attachment_filename='accidents.csv')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
