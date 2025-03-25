import pymysql
import requests
from datetime import datetime
import pytz
import os
import time
from flask import Flask, jsonify
import threading

# MySQL Database Connection Details (Replace with your FreeSQL details)
DB_CONFIG = {
    "host": "sql12.freesqldatabase.com",
    "user": "sql12769381",
    "password": "sl3Frk1Qjk",
    "database": "sql12769381"
}

API_KEY = "c79e3c246371e187fbe3bb19688264cd"
CITY = "Kuching"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

app = Flask(__name__)

# Set Malaysia Timezone
malaysia_tz = pytz.timezone("Asia/Kuala_Lumpur")

# Function to insert data into MySQL
def insert_weather_data(temp, humidity, wind_speed):
    try:
        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        sql = "INSERT INTO weather_data (timestamp, temperature, humidity, wind_speed) VALUES (%s, %s, %s, %s)"
        
        # Get the current timestamp in UTC
        timestamp = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute(sql, (timestamp, temp, humidity, wind_speed))
        conn.commit()
        cursor.close()
        conn.close()
    except pymysql.MySQLError as err:
        print("Error:", err)

# Function to fetch weather data and store in MySQL every 5 minutes
def fetch_and_store_weather():
    while True:
        response = requests.get(URL)
        data = response.json()

        if response.status_code == 200:
            temp = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            wind_speed = data["wind"]["speed"]

            insert_weather_data(temp, humidity, wind_speed)
            print(f"Logged: {datetime.now(pytz.utc)} - Temp: {temp}°C, Humidity: {humidity}%, Wind Speed: {wind_speed} m/s")

        else:
            print("Error fetching data:", data)

        time.sleep(10)  # Wait 5 minutes

# Flask endpoint to check stored weather data
@app.route("/weather", methods=["GET"])
def get_weather_data():
    try:
        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 10")  # Fetch last 10 records
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(result)
    except pymysql.MySQLError as err:
        return jsonify({"error": str(err)})

if __name__ == "__main__":
    # Start the weather data fetching in a background thread
    threading.Thread(target=fetch_and_store_weather, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=True)
