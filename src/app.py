import os
from flask import Flask, jsonify, request, send_file
import pandas as pd
import schedule
import time
import threading
from data_access import DataAccessLayer
from socrata_client import SocrataClient
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.join(ROOT_DIR, 'app')
DATA_DIR = os.path.join(APP_DIR, 'data')

if not os.path.exists(APP_DIR):
    os.makedirs(APP_DIR)
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

app = Flask(__name__)
data_access_layer = DataAccessLayer()
socrata_client = SocrataClient()
DATASET_ID = "eax3-qev8"

@app.route('/accidents/emptyFile')
def create_empty_csv():
    try:
        df = pd.DataFrame(list())
        DataAccessLayer.save_data(df)
        return "Arquivo criado com sucesso"
    except Exception as e:
        logger.error(f"Error creating empty file: {e}")
        return f"Error creating empty file: {e}", 500

@app.route('/accidents/downloadFile')
def download_csv():
    try:
        return send_file(DataAccessLayer.FILE_PATH, as_attachment=True, download_name="accidents.csv")
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return f"Error downloading file: {e}", 500

@app.route('/accidents/testJob')
def test_job():
    try:
        results = socrata_client.get_last_24_hours_data(DATASET_ID)
        if results:
            results_df = pd.DataFrame.from_records(results)
            combined_df = DataAccessLayer.add_new_data(results_df)
            return jsonify({"status": "Data fetched and CSV updated", "data_count": len(results)})
        else:
            return jsonify({"status": "No data retrieved"}), 500
    except Exception as e:
        logger.error(f"Error in testJob: {e}")
        return jsonify({"status": f"Error in testJob: {e}"}), 500

@app.route('/accidents/data', methods=['GET'])
def get_all_data():
    try:
        data = DataAccessLayer.get_data()
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error retrieving data: {e}")
        return jsonify({"status": f"Error retrieving data: {e}"}), 500

@app.route('/accidents/data/<record_id>', methods=['GET'])
def get_data_by_id(record_id):
    try:
        data = DataAccessLayer.get_data_by_id(record_id)
        if data:
            return jsonify(data), 200
        else:
            return jsonify({"status": "Record not found"}), 404
    except Exception as e:
        logger.error(f"Error retrieving data: {e}")
        return jsonify({"status": f"Error retrieving data: {e}"}), 500

@app.route('/accidents/data/<record_id>', methods=['DELETE'])
def delete_data_by_id(record_id):
    try:
        data = DataAccessLayer.delete_data_by_id(record_id)
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Error deleting data: {e}")
        return jsonify({"status": f"Error deleting data: {e}"}), 500

@app.route('/accidents/datasetId', methods=['GET'])
def get_dataset_id():
    return jsonify({"datasetId": DATASET_ID}), 200

@app.route('/accidents/resourceStatus', methods=['GET'])
def check_resource_status():
    try:
        data_exists = os.path.exists(DataAccessLayer.FILE_PATH) and os.path.getsize(DataAccessLayer.FILE_PATH) > 0
        status = "Repository is being fed" if data_exists else "Repository is empty"
        return jsonify({"status": status}), 200
    except Exception as e:
        logger.error(f"Error checking resource status: {e}")
        return jsonify({"status": f"Error checking resource status: {e}"}), 500

@app.route('/accidents/filter', methods=['POST'])
def filter_data():
    try:
        filter_params = request.json
        df = DataAccessLayer.load_existing_data()
        
        for key, value in filter_params.items():
            if key in df.columns:
                df = df[df[key].astype(str).str.contains(value, case=False, na=False)]
        
        if df.empty:
            return jsonify({"status": "No data found matching the criteria"}), 404
        
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        logger.error(f"Error filtering data: {e}")
        return jsonify({"status": f"Error filtering data: {e}"}), 500
    
@app.route('/accidents/total', methods=['GET'])
def get_total_accidents():
    try:
        data = DataAccessLayer.get_data()
        total_accidents = len(data)
        return jsonify({"total_accidents": total_accidents}), 200
    except Exception as e:
        logger.error(f"Error retrieving total accidents: {e}")
        return jsonify({"status": f"Error retrieving total accidents: {e}"}), 500

def job():
    try:
        results = socrata_client.get_last_24_hours_data(DATASET_ID)
        if results:
            results_df = pd.DataFrame.from_records(results)
            DataAccessLayer.add_new_data(results_df)
    except Exception as e:
        logger.error(f"Error in job execution: {e}")

schedule.every(1).minutes.do(job)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/accidents', methods=['GET'])
def get_accidents():
    return "Servidor conectado"

if __name__ == '__main__':
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=5000)
