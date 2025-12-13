import json
from airflow.models import BaseOperator
from hook.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):
    def __init__(self, start_time: str, end_time: str, query: str, **kwargs):
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        super().__init__(**kwargs)

    def execute(self, context):
        with open("./data/twitter/extract_twitter.json", "w", encoding='utf-8') as output_file:
            for pg in TwitterHook(self.start_time, self.end_time, self.query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n") 