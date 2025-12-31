import json
from airflow.models import BaseOperator
from hook.weather_hook import WeatherHook
from datetime import datetime

class WeatherOperator(BaseOperator):
    def __init__(self, start_time: str, end_time: str, city: str, path: str, **kwargs):
        self.start_time = start_time
        self.end_time = end_time
        self.city = city
        self.path = path
        super().__init__(**kwargs)

    def execute(self, context):        
        filename = datetime.now().strftime("%Y%m%d")
        with open(f"{self.path}/{filename}.json", "w", encoding='utf-8') as output_file:
            for pg in WeatherHook(self.start_time, self.end_time, self.city).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n") 