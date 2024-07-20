import os
from flask import Flask, jsonify, request, send_file
import pandas as pd
import schedule
import time
import threading
from src.data_access import DataAccessLayer
from src.socrata_client import SocrataClient

app = Flask(__name__)
data_access_layer = DataAccessLayer()
socrata_client = SocrataClient()

@app.route('/accidents/emptyFile')
def create_empty_csv():
    try:
        df = pd.DataFrame(list())
        DataAccessLayer.save_data(df)
        return "Arquivo criado com sucesso"
    except Exception as e:
        return f"Error creating empty file: {e}", 500

@app.route('/accidents/downloadFile')
def download_csv():
    try:
        return send_file(DataAccessLayer.FILE_PATH, as_attachment=True, download_name="accidents.csv")
    except Exception as e:
        return f"Error downloading file: {e}", 500

@app.route('/accidents/testJob')
def test_job():
    try:
        results = socrata_client.get_last_24_hours_data("eax3-qev8")
        if results:
            results_df = pd.DataFrame.from_records(results)
            combined_df = DataAccessLayer.add_new_data(results_df)
            return jsonify({"status": "Data fetched and CSV updated", "data_count": len(results)})
        else:
            return jsonify({"status": "No data retrieved"}), 500
    except Exception as e:
        return jsonify({"status": f"Error in testJob: {e}"}), 500

@app.route('/accidents/data', methods=['GET'])
def get_all_data():
    try:
        data = DataAccessLayer.get_data()
        return jsonify(data), 200
    except Exception as e:
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
        return jsonify({"status": f"Error retrieving data: {e}"}), 500

@app.route('/accidents/data/<record_id>', methods=['DELETE'])
def delete_data_by_id(record_id):
    try:
        data = DataAccessLayer.delete_data_by_id(record_id)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"status": f"Error deleting data: {e}"}), 500

def job():
    try:
        results = socrata_client.get_last_24_hours_data("eax3-qev8")
        if results:
            results_df = pd.DataFrame.from_records(results)
            DataAccessLayer.add_new_data(results_df)
    except Exception as e:
        print(f"Error in job execution: {e}")

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
