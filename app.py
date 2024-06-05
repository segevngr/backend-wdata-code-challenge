import asyncio
import csv
import logging
import os

from quart import Quart, request, jsonify
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.errors import PyMongoError

# CSV Files columns
LATITUDE_COLUMN = "Latitude"
LONGITUDE_COLUMN = "Longitude"
TIME_COLUMN = "forecast_time"
TEMP_CELSIUS_COLUMN = "Temperature Celsius"
PRECIPITATION_MM_COLUMN = "Precipitation Rate mm/hr"
PRECIPITATION_IN_COLUMN = "Precipitation Rate in/hr"
VERY_HOT_THRESHOLD = 30
COLD_AND_RAINY_TEMP_THRESHOLD = 10
RAIN_THRESHOLD_MM = 0.5

# Relative path to the dir which holds the CSV files
DATA_FILES_PATH = '/data'

# Size of the data chunks to be read\write separately
BUFFER_SIZE = 100000

CSV_ROW_COUNT = 259201

app = Quart(__name__)
app.logger.setLevel(logging.INFO)

# MongoDB connection
MONGO_URI = os.getenv('MONGO_URI')
client = AsyncIOMotorClient(MONGO_URI)
db = client['weather_db']
collection = db['weather_collection']


# CSV rows generator to extract the next rows from the given start index
def read_csv_from_row(file_path, start_row):
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        # Skips rows:
        for _ in range(start_row):
            next(reader)
        for row in reader:
            yield row


# Read a chunk from the CSV and write it to the db
async def read_csv_and_write_to_db(semaphore, csv_file_path: str, start_row: int):
    async with semaphore:
        csv_data_generator = read_csv_from_row(csv_file_path, start_row)
        rows = []
        for _ in range(BUFFER_SIZE):
            try:
                rows.append(next(csv_data_generator))
            except StopIteration:
                break

        await collection.insert_many(rows)
        app.logger.info(f"Wrote {len(rows)} rows to db")


# Generates asynchronous tasks lists for reading the CSVs and writing them to Mongo db
def generate_read_and_write_tasks(data_folder_path):
    csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv')]
    if not csv_files:
        return {'error': 'No CSV files found in the folder'}, 400

    semaphore = asyncio.Semaphore(4)
    tasks = []
    for csv_file in csv_files:
        csv_file_path = os.path.join(data_folder_path, csv_file)
        start_row = 0
        while start_row < CSV_ROW_COUNT:
            tasks.append(asyncio.create_task(read_csv_and_write_to_db(semaphore, csv_file_path, start_row)))
            start_row += BUFFER_SIZE

    return tasks


@app.route('/load_to_db')
async def load_data_to_db():
    data_folder_path = os.getcwd() + DATA_FILES_PATH
    if not os.path.exists(data_folder_path):
        return jsonify({'error': f'Data folder path does not exist: {data_folder_path}'}), 400

    try:
        await collection.create_index([(LATITUDE_COLUMN, ASCENDING), (LONGITUDE_COLUMN, ASCENDING)])
        tasks = generate_read_and_write_tasks(data_folder_path)
        app.logger.info("Reading data and writing to db...")
        await asyncio.gather(*tasks)
        app.logger.info("Finished Writing to db")
        return jsonify({'message': 'Data successfully loaded to the database'}), 200

    except PyMongoError as e:
        app.logger.error(f"Error writing to database: {e}")
        return jsonify({'error': 'Error writing to database'}), 500
    except Exception as e:
        app.logger.error(f"Error processing request: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/weather/insight', methods=['GET'])
async def weather_insight():
    condition = request.args.get('condition')
    lat = request.args.get('lat')
    lon = request.args.get('lon')

    if not all([condition, lat, lon]):
        return jsonify({'error': 'Missing query parameters'}), 400

    if condition not in ('veryHot', 'rainyAndCold'):
        return jsonify({'error': 'Invalid condition'}), 400

    try:
        app.logger.info("Querying DB...")
        query = {LATITUDE_COLUMN: lat, LONGITUDE_COLUMN: lon}
        results = collection.find(query)
        app.logger.info("Query results returned successfully")

        response = []
        async for result in results:
            time = result[TIME_COLUMN]
            temp = float(result[TEMP_CELSIUS_COLUMN])

            if PRECIPITATION_IN_COLUMN in result:
                precipitation = float(result[PRECIPITATION_IN_COLUMN]) * 25.4  # Converting in/hr to mm/hr
            else:
                precipitation = float(result[PRECIPITATION_MM_COLUMN])

            if condition == "veryHot":
                condition_met = temp > VERY_HOT_THRESHOLD
            else:
                condition_met = temp < COLD_AND_RAINY_TEMP_THRESHOLD and precipitation > RAIN_THRESHOLD_MM

            response.append({'forecastTime': time, 'conditionMet': condition_met})

        return jsonify(response)

    except PyMongoError as e:
        app.logger.error(f"Database query error: {e}")
        return jsonify({'error': 'Database query error'}), 500
    except Exception as e:
        app.logger.error(f"Error processing request: {e}")
        return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
