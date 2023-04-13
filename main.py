import requests
import json

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class TripUpdate(Base):
    __tablename__ = 'trip_updates'

    id = Column(String, primary_key=True)

    trip_id = Column(String)
    start_time = Column(String)
    start_date = Column(String)
    schedule_relationship = Column(String)
    route_id = Column(String)
    direction_id = Column(Integer)

    stop_time_updates = relationship('StopTimeUpdate', backref='trip_update')

class StopTimeUpdate(Base):
    __tablename__ = 'stop_time_updates'

    # Custom unique id which is the TripUpdate id concatenated to
    # the stopSequence (with an underscore in between). Something like this
    # is necessary since no unique id is provided in the API.
    id = Column(String, primary_key=True)

    stop_id = Column(String)
    stop_sequence = Column(Integer)
    arrival_time = Column(String)
    departure_time = Column(String)
    schedule_relationship = Column(String)

    trip_update_id = Column(String, ForeignKey('trip_updates.id'))

def get_json_data(api_key, format):
    url = f'https://api.goswift.ly/real-time/vta/gtfs-rt-trip-updates?apiKey={api_key}&format={format}'

    response = requests.get(url)
    data = json.loads(response.text)

    return data

def parse_feed(api_key, format, session):
    data = get_json_data(api_key, format)

    trip_updates = data['entity']

    for trip_update in trip_updates:
        trip = trip_update['tripUpdate']['trip']

        trip_update_entity = TripUpdate(
            id = trip_update['id'],
            trip_id = trip['tripId'],
            start_time = trip['startTime'],
            start_date = trip['startDate'],
            schedule_relationship = trip['scheduleRelationship'],
            route_id = trip['routeId'],
            direction_id = trip['directionId']
        )

        # Some trip updates don't have a stopTimeUpdate
        if 'stopTimeUpdate' not in trip_update['tripUpdate']: continue

        for stop_time_update in trip_update['tripUpdate']['stopTimeUpdate']:
            stop_time_update_entity = StopTimeUpdate(
                id = f"{trip_update_entity.id}_{stop_time_update['stopSequence']}",
                stop_id = stop_time_update['stopId'],
                stop_sequence = stop_time_update['stopSequence'],
                schedule_relationship = stop_time_update['scheduleRelationship'],
                trip_update_id = trip_update_entity.id
            )

            # The arrival and departure times are optional so we need to check if they exist

            if 'arrival' in stop_time_update:
                stop_time_update_entity.arrival_time = stop_time_update['arrival']['time']

            if 'departure' in stop_time_update:
                stop_time_update_entity.departure_time = stop_time_update['departure']['time']

            session.add(stop_time_update_entity)

        session.add(trip_update_entity)

    session.commit()

if __name__ == '__main__':
    engine = create_engine('sqlite:///gtfs.sqlite', echo=True)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    parse_feed('59af72683221a1734f637eae7a7e8d9b', 'json', session)
