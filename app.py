import csv
import os
from typing import List, Dict, Any

from flask import Flask, jsonify, Response
from pymongo import MongoClient

DATA_FILES_PATH = '\data\\'

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb+srv://segevngr:pass123@cluster0.lwqch8m.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['weather_db']
collection = db['weather_collection']


def read_csv_file(filename: str) -> List[Dict[str, Any]]:
    data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data


@app.route('/load_to_db')
def load_csvs_to_db() -> Response:
    data_folder_path = os.path.dirname(os.path.abspath(__file__)) + DATA_FILES_PATH
    print(data_folder_path)
    if not os.path.exists(data_folder_path):
        return jsonify({'error': 'Data folder does not exist'})

    csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv')]
    if not csv_files:
        return jsonify({'error': 'No CSV files found in the folder'})

    app.logger.debug("Writing data to DB...")
    for csv_file in csv_files:
        csv_file_path = os.path.join(data_folder_path, csv_file)
        csv_data = read_csv_file(csv_file_path)
        collection.insert_many(csv_data)

    return jsonify({'message': 'Weather data stored in db successfully'})


if __name__ == '__main__':
    app.run(debug=True)


