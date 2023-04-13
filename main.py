import requests
import json

from sqlalchemy import create_engine

def parse_feed(api_key, format):
    url = f'https://api.goswift.ly/real-time/vta/gtfs-rt-trip-updates?apiKey={api_key}&format={format}'

    response = requests.get(url)
    data = json.loads(response.text)
    trip_updates = data['entity']

    for trip_update in trip_updates:
        print(trip_update)

if __name__ == '__main__':
    parse_feed('59af72683221a1734f637eae7a7e8d9b', 'json')

    engine = create_engine('sqlite:///gtfs.sqlite', echo=True)

    conn = engine.connect()
