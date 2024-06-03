from flask import request, jsonify
from pymongo.errors import PyMongoError

from app import app, collection

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


@app.route('/weather/insight', methods=['GET'])
def weather_insight():
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
        for result in results:
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



