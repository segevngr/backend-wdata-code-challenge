import logging
import os

from flask import Flask
from motor.motor_asyncio import AsyncIOMotorClient


app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# MongoDB connection
MONGO_URI = os.getenv('MONGO_URI')
client = AsyncIOMotorClient(MONGO_URI)
db = client['weather_db']
collection = db['weather_collection']

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
