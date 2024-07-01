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

# Função para obter todos os dados do MongoDB
def get_all_data_from_mongo(collection_name):
    db = mongo_client.Socrata
    collection = db[collection_name]
    results = collection.find()
    print(f"Results from MongoDB: {list(results)}")
    return list(results)

class DataAdapter:
    @staticmethod
    def format_data_to_csv(data):
        if not data:
            print("No data retrieved from MongoDB")
        else:
            print(f"Data retrieved: {data}")

        df = pd.DataFrame(data)
        if df.empty:
            print("DataFrame is empty")
        else:
            print(f"DataFrame created with {len(df)} records\n{df.head()}")

        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return output

@app.route('/accidents', methods=['GET'])
def get_accidents():
    # Obtenha todos os resultados do MongoDB
    results = get_all_data_from_mongo("accidents_prf_collection")
    # Formate os dados em CSV usando o adapter
    csv_data = DataAdapter.format_data_to_csv(results)
    return send_file(io.BytesIO(csv_data.getvalue().encode()), mimetype='text/csv', as_attachment=True, download_name='accidents.csv')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
