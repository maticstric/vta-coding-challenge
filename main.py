import requests
import json

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///gtfs.sqlite'
db = SQLAlchemy(app)

class TripUpdate(db.Model):
    __tablename__ = 'trip_updates'

    id = db.Column(db.String, primary_key=True)

    trip_id = db.Column(db.String)
    start_time = db.Column(db.String)
    start_date = db.Column(db.String)
    schedule_relationship = db.Column(db.String)
    route_id = db.Column(db.String)
    direction_id = db.Column(db.Integer)
    timestamp = db.Column(db.String)
    vehicle_id = db.Column(db.String)

    stop_time_updates = db.relationship('StopTimeUpdate', backref='trip_update')

    def dict_format(self):
        # Converts this object back into the JSON/dict form from the API

        d = {
                'tripUpdate': {
                    'trip': {}
                }
            }

        d['id'] = self.id
        d['tripUpdate']['trip']['tripId'] = self.trip_id
        d['tripUpdate']['trip']['startTime'] = self.start_time
        d['tripUpdate']['trip']['startDate'] = self.start_date
        d['tripUpdate']['trip']['scheduleRelationship'] = self.schedule_relationship
        d['tripUpdate']['trip']['routeId'] = self.route_id
        d['tripUpdate']['trip']['directionId'] = self.direction_id

        records = db.session.query(StopTimeUpdate).all()

        for record in records:
            if record.trip_update_id == self.id:
                if 'stopTimeUpdate' not in d['tripUpdate']:
                    d['tripUpdate']['stopTimeUpdate'] = []

                d['tripUpdate']['stopTimeUpdate'].append(record.dict_format())

        if self.vehicle_id != None:
            d['tripUpdate']['vehicle'] = { 'id': self.vehicle_id }

        d['tripUpdate']['timestamp'] = self.timestamp

        return d

class StopTimeUpdate(db.Model):
    __tablename__ = 'stop_time_updates'

    # Custom unique id which is the TripUpdate id concatenated to
    # the stopSequence (with an underscore in between). Something like this
    # is necessary since no unique id is provided in the API.
    id = db.Column(db.String, primary_key=True)

    stop_id = db.Column(db.String)
    stop_sequence = db.Column(db.Integer)
    arrival_time = db.Column(db.String)
    departure_time = db.Column(db.String)
    schedule_relationship = db.Column(db.String)

    trip_update_id = db.Column(db.String, db.ForeignKey('trip_updates.id'))

    def dict_format(self):
        # Converts this object back into the JSON/dict form from the API

        d = {}

        d['stopSequence'] = self.stop_sequence
        d['stopId'] = self.stop_id

        if self.arrival_time:
            d['arrival'] = { 'time': self.arrival_time }

        if self.departure_time:
            d['departure'] = { 'time': self.departure_time }

        d['scheduleRelationship'] = self.schedule_relationship

        return d

def get_json_data(api_key, format):
    url = f'https://api.goswift.ly/real-time/vta/gtfs-rt-trip-updates?apiKey={api_key}&format={format}'

    response = requests.get(url)
    data = json.loads(response.text)

    return data

def add_trip_update_entity(trip_update_entity):
    exists = db.session.query(TripUpdate.id).filter_by(id=trip_update_entity.id).scalar() is not None

    # According to the instructions, "Existing records within the database
    # should be skipped, and new ones should be appended."

    if not exists:
        db.session.add(trip_update_entity)

def add_stop_time_update_entity(stop_time_update_entity):
    exists = db.session.query(StopTimeUpdate.id).filter_by(id=stop_time_update_entity.id).scalar() is not None

    # According to the instructions, "Existing records within the database
    # should be skipped, and new ones should be appended."

    if not exists:
        db.session.add(stop_time_update_entity)

# TODO Remove the StopTimeUpdates for expired TripUpdates as well
def delete_expired_records(data):
    records = db.session.query(TripUpdate).all()

    for record in records:
        record_expired = True

        for trip_update in data['entity']:
            if trip_update['id'] == record.id:
                record_expired = False

        if record_expired:
            db.session.query(TripUpdate).filter(TripUpdate.id==record.id).delete()

    db.session.commit()

def parse_feed(data):
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
            direction_id = trip['directionId'],
            timestamp = trip_update['tripUpdate']['timestamp']
        )

        # vehicle id is optional (for CANCELED) so we need to check if it exists
        if 'vehicle' in trip_update['tripUpdate']:
            trip_update_entity.vehicle_id = trip_update['tripUpdate']['vehicle']['id']

        # stopTimeUpdate is optional (for CANCELED) so we need to check if it exists
        if 'stopTimeUpdate' in trip_update['tripUpdate']:
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

                
                add_stop_time_update_entity(stop_time_update_entity)

        add_trip_update_entity(trip_update_entity)

    db.session.commit()

if __name__ == '__main__':

    data = get_json_data('59af72683221a1734f637eae7a7e8d9b', 'json')

    with app.app_context():
        db.create_all()

        delete_expired_records(data)
        parse_feed(data)

        #trip_updates = db.session.query(TripUpdate).limit(100).all()
        #result = [tu.dict_format() for tu in trip_updates]
        #print(jsonify(result).get_data())
