import csv
import os

from flask import jsonify
from pymongo import ASCENDING
from pymongo.errors import PyMongoError

from app import app, collection
from query_data import LATITUDE_COLUMN, LONGITUDE_COLUMN


# Relative path to the dir which holds the CSV files
DATA_FILES_PATH = '/data'

# Determines what's the max rows of data to be stored in memory at once
BUFFER_SIZE = 100000


def read_csv_file_stream(filename: str):
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row


@app.route('/load_to_db')
def load_csvs_to_db():
    data_folder_path = os.getcwd() + DATA_FILES_PATH
    if not os.path.exists(data_folder_path):
        return jsonify({'error': f'Data folder path does not exist: {data_folder_path}'}), 400

    csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv')]
    if not csv_files:
        return jsonify({'error': 'No CSV files found in the folder'}), 400

    try:
        app.logger.info("Writing data to db...")
        for csv_file in csv_files:
            csv_file_path = os.path.join(data_folder_path, csv_file)
            csv_data_generator = read_csv_file_stream(csv_file_path)
            buffer = []
            buffer_count = 0

            for row in csv_data_generator:
                buffer.append(row)
                if len(buffer) >= BUFFER_SIZE:
                    collection.insert_many(buffer)
                    buffer = []
                    buffer_count += 1
                    app.logger.info(f'Wrote {buffer_count*BUFFER_SIZE} rows to db')

            # Insert any remaining rows in the buffer
            if buffer:
                collection.insert_many(buffer)
        app.logger.info("Finished Writing to db")

        app.logger.info(f'Indexing db...')
        collection.create_index([(LONGITUDE_COLUMN, ASCENDING), (LATITUDE_COLUMN, ASCENDING)])
        app.logger.info(f'Finished Indexing db')

        return jsonify({'message': 'Weather data stored in db successfully'})

    except PyMongoError as e:
        app.logger.error(f"Error writing to database: {e}")
        return jsonify({'error': 'Error writing to database'}), 500
    except Exception as e:
        app.logger.error(f"Error processing request: {e}")
        return jsonify({'error': 'Internal server error'}), 500

