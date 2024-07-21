import os
import pandas as pd

class DataAccessLayer:
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(ROOT_DIR, 'app', 'data')
    FILE_PATH = os.path.join(DATA_DIR, 'accidents.csv')

    @staticmethod
    def ensure_data_dir_exists():
        if not os.path.exists(DataAccessLayer.DATA_DIR):
            os.makedirs(DataAccessLayer.DATA_DIR)

    @staticmethod
    def load_existing_data():
        DataAccessLayer.ensure_data_dir_exists()
        if os.path.exists(DataAccessLayer.FILE_PATH) and os.path.getsize(DataAccessLayer.FILE_PATH) > 0:
            try:
                df = pd.read_csv(DataAccessLayer.FILE_PATH)

                if df.empty or not all(col in df.columns for col in ["Id"]):
                    raise ValueError("CSV file is empty or missing required columns.")
                return df
            except Exception as e:
                print(f"Error loading data: {e}")
                return pd.DataFrame()
        else:
            return pd.DataFrame()

    @staticmethod
    def save_data(df):
        df.to_csv(DataAccessLayer.FILE_PATH, index=False)

    @staticmethod
    def add_new_data(new_data_df):
        existing_df = DataAccessLayer.load_existing_data()
        combined_df = pd.concat([existing_df, new_data_df]).drop_duplicates(subset="Id")
        DataAccessLayer.save_data(combined_df)
        return combined_df

    @staticmethod
    def get_data():
        return DataAccessLayer.load_existing_data().to_dict(orient="records")

    @staticmethod
    def get_data_by_id(record_id):
        df = DataAccessLayer.load_existing_data()
        record = df[df["Id"] == record_id]
        if not record.empty:
            return record.to_dict(orient="records")[0]
        return None

    @staticmethod
    def delete_data_by_id(record_id):
        df = DataAccessLayer.load_existing_data()
        df = df[df["Id"] != record_id]
        DataAccessLayer.save_data(df)
        return df.to_dict(orient="records")
