from datetime import datetime
import os
import requests
from typing import Dict, Any
from airflow.models import Variable

# openweathermap_api_key = 'bd8b3dce0a905f2df4bcdcbf7a34618d'
class OpenWeatherMapAPIClass:
    def __init__(self):
        #self.api_key = os.getenv('openweathermap_api_key')
        self.api_key = Variable.get('openweathermap_api_key')
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"

    def get_weather_data(self, city_name: str) -> Dict[str, Any]:
        params = {
            "q": city_name,
            "appid": self.api_key,
            "units": "metric"  # Use metric units for temperature in Celsius
        }
        
        response = requests.get(self.base_url, params=params)
        #response.raise_for_status()  # Raise an exception for HTTP errors
        
        return response.json()

    def get_formatted_weather_data(self, city_name: str) -> Dict[str, Any]:
        data = self.get_weather_data(city_name)
    

        return {
            "city": data["name"],
            "country": data["sys"]["country"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "created_timestamp": datetime.now().isoformat()

        }
    
    

# Usage example:
# api_key = "your_openweathermap_api_key"
# weather_api = OpenWeatherMapAPI(api_key)
# weather_data = weather_api.get_formatted_weather_data("London")
# print(weather_data)
