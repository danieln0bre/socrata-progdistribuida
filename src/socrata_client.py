from sodapy import Socrata
from datetime import datetime, timedelta

columns_to_keep = [
    "Id", "Event_Number", "Type", "Severity", "Description",
    "Address", "City", "State", "Zip", "CreationTime", "UpdateTime"
]

class SocrataClient:
    def __init__(self):
        self.client = Socrata("data.fortworthtexas.gov", None)

    def get_last_24_hours_data(self, dataset_id):
        try:
            last_24_hours = datetime.now() - timedelta(days=1)
            last_24_hours_str = last_24_hours.strftime("%Y-%m-%dT%H:%M:%S.%f")

            query = f"creationtime >= '{last_24_hours_str}'"
            results = self.client.get(dataset_id, where=query, limit=20)

            if not results:
                return []

            processed_results = []
            for record in results:
                processed_record = {key: record.get(key.lower(), "") for key in columns_to_keep}
                processed_results.append(processed_record)

            return processed_results
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []
