import csv
import logging
import os
from typing import List, Dict, Any

from flask import Flask, jsonify, Response, request
from pymongo import MongoClient, ASCENDING

DATA_FILES_PATH = '\data\\'

# Data column names
LATITUDE_COLUMN = "Latitude"
LONGITUDE_COLUMN = "Longitude"
TIME_COLUMN = "forecast_time"
TEMP_CELSIUS_COLUMN = "Temperature Celsius"
PRECIPITATION_MM_COLUMN = "Precipitation Rate mm/hr"
PRECIPITATION_IN_COLUMN = "Precipitation Rate in/hr"

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

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
    if not os.path.exists(data_folder_path):
        return jsonify({'error': 'Data folder does not exist'})

    csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv')]
    if not csv_files:
        return jsonify({'error': 'No CSV files found in the folder'})

    app.logger.info("Writing data to DB...")
    for csv_file in csv_files:
        csv_file_path = os.path.join(data_folder_path, csv_file)
        csv_data = read_csv_file(csv_file_path)
        collection.insert_many(csv_data)

    collection.create_index([(LONGITUDE_COLUMN, ASCENDING), (LATITUDE_COLUMN, ASCENDING)])

    return jsonify({'message': 'Weather data stored in db successfully'})


@app.route('/weather/insight', methods=['GET'])
def weather_insight():
    condition = request.args.get('condition')
    lat = request.args.get('lat')
    lon = request.args.get('lon')

    if not all([condition, lat, lon]):
        return jsonify({'error': 'Missing query parameters'}), 400

    if condition not in ('veryHot', 'rainyAndCold'):
        return jsonify({'error': 'Invalid condition'}), 400

    app.logger.info("Querying DB...")
    query = {"Latitude": lat, "Longitude": lon}
    results = collection.find(query)
    app.logger.info("Query results returned successfully")

    response = []
    for result in results:
        time = result[TIME_COLUMN]
        temp = float(result[TEMP_CELSIUS_COLUMN])

        if PRECIPITATION_IN_COLUMN in result:
            precipitation = float(result[PRECIPITATION_IN_COLUMN]) * 25.4  # Converting to in/hr to mm/hr
        else:
            precipitation = float(result[PRECIPITATION_MM_COLUMN])

        if condition == "veryHot":
            condition_met = temp > 30
        else:
            condition_met = temp < 10 and precipitation > 0.5

        response.append({'forecastTime': time, 'conditionMet': condition_met})

    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
