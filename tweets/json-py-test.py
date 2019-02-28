import json
from pprint import pprint

with open('shakespeare-tweets.json') as f:
    data = json.load(f)
    pprint(data)
