import os
from flask import Flask, jsonify, request, send_file
from sodapy import Socrata
import pandas as pd
from datetime import datetime, timedelta
import schedule
import time
import threading

app = Flask(__name__)

# Initialize Socrata client
client = Socrata("data.fortworthtexas.gov", None)
columns_to_keep = [
    "Id", "Event_Number", "Type", "Severity", "Description",
    "Address", "City", "State", "Zip", "CreationTime", "UpdateTime"
]

DATA_DIR = '/app/data'  # Directory with correct permissions

def get_last_24_hours_data(dataset_id, client):
    try:
        last_24_hours = datetime.now() - timedelta(days=1)
        last_24_hours_str = last_24_hours.strftime("%Y-%m-%dT%H:%M:%S.%f")

        query = f"creationtime >= '{last_24_hours_str}'"
        print(f"Query: {query}")  # Debugging: Print the query
        results = client.get(dataset_id, where=query, limit=20)
        print(f"Results: {results}")  # Debugging: Print the results

        if not results:
            print("No data fetched from the API.")
            return []

        # Process the results to match the expected columns
        processed_results = []
        for record in results:
            processed_record = {}
            for key in columns_to_keep:
                processed_record[key] = record.get(key.lower(), "")  # Use lower case to match API keys
            processed_results.append(processed_record)

        return processed_results
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

@app.route('/accidents/emptyFile')
def create_empty_csv():
    try:
        df = pd.DataFrame(list())
        df.to_csv(os.path.join(DATA_DIR, 'accidents.csv'))
        return "Arquivo criado com sucesso"
    except Exception as e:
        return f"Error creating empty file: {e}", 500

@app.route('/accidents/downloadFile')
def download_csv():
    try:
        file_path = os.path.join(DATA_DIR, 'accidents.csv')
        return send_file(file_path, as_attachment=True, download_name="accidents.csv")
    except Exception as e:
        return f"Error downloading file: {e}", 500

@app.route('/accidents/testJob')
def test_job():
    try:
        results = get_last_24_hours_data("eax3-qev8", client)
        if results:
            results_df = pd.DataFrame.from_records(results)
            print(f"DataFrame: {results_df}")  # Debugging: Print the DataFrame
            results_df.to_csv(os.path.join(DATA_DIR, 'accidents.csv'), index=False)
            return jsonify({"status": "Data fetched and CSV created", "data_count": len(results)})
        else:
            return jsonify({"status": "No data retrieved"}), 500
    except Exception as e:
        print(f"Error in testJob: {e}")
        return jsonify({"status": f"Error in testJob: {e}"}), 500

def job():
    try:
        print("Running scheduled job...")  # Debugging: Indicate job is running
        results = get_last_24_hours_data("eax3-qev8", client)
        if results:  # Check if results are not empty
            results_df = pd.DataFrame.from_records(results)
            print(f"DataFrame: {results_df}")  # Debugging: Print the DataFrame
            results_df.to_csv(os.path.join(DATA_DIR, 'accidents.csv'), index=False)
            print("CSV file created successfully.")  # Debugging: Indicate CSV creation
        else:
            print("No data retrieved")  # Debugging: Print message if no data
    except Exception as e:
        print(f"Error in job execution: {e}")

schedule.every(1).minutes.do(job)

def run_scheduler():
    print("Scheduler thread started")  # Debugging: Indicate scheduler thread started
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
    print("Flask app running")  # Debugging: Indicate Flask app is running
    app.run(host='0.0.0.0', port=5000)
