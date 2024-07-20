import os
import pandas as pd

DATA_DIR = '/app/data'
FILE_PATH = os.path.join(DATA_DIR, 'accidents.csv')

class DataAccessLayer:
    @staticmethod
    def load_existing_data():
        if os.path.exists(FILE_PATH):
            return pd.read_csv(FILE_PATH)
        return pd.DataFrame()

    @staticmethod
    def save_data(df):
        df.to_csv(FILE_PATH, index=False)

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
