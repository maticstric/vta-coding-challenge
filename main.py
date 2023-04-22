import json
import sys
import time

from argparse import ArgumentParser
import requests
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import create_engine, text

import pymysql
pymysql.install_as_MySQLdb()

VERBOSITY = 0

app = Flask(__name__)
db = SQLAlchemy()

class TripUpdate(db.Model):
    __tablename__ = 'trip_updates'

    id = db.Column(db.String(64), primary_key=True)

    trip_id = db.Column(db.String(64))
    start_time = db.Column(db.String(64))
    start_date = db.Column(db.String(64))
    schedule_relationship = db.Column(db.String(64))
    route_id = db.Column(db.String(64))
    direction_id = db.Column(db.Integer)
    timestamp = db.Column(db.String(64))
    vehicle_id = db.Column(db.String(64))

    stop_time_updates = db.relationship('StopTimeUpdate', cascade='all, delete', backref='trip_update')

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
    id = db.Column(db.String(64), primary_key=True)

    stop_id = db.Column(db.String(64))
    stop_sequence = db.Column(db.Integer)
    arrival_time = db.Column(db.String(64))
    departure_time = db.Column(db.String(64))
    schedule_relationship = db.Column(db.String(64))

    trip_update_id = db.Column(db.String(64), db.ForeignKey('trip_updates.id'))

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

    if response.status_code != 200:
        sys.exit('Error: Request to API failed. Make sure API key is correct.')

    data = json.loads(response.text)

    return data

def delete_expired_records(data):
    new_ids = [ tu['id'] for tu in data['entity'] ]

    stmt = StopTimeUpdate.__table__.delete().where(StopTimeUpdate.trip_update_id.not_in(new_ids))
    db.session.execute(stmt)

    stmt = TripUpdate.__table__.delete().where(TripUpdate.id.not_in(new_ids))
    db.session.execute(stmt)

    db.session.commit()

def add_new_trip_updates(data, old_ids):
    to_add = []

    # Reformat the data into a dictonary and make a list of them to add
    for trip_update in data['entity']:
        if trip_update['id'] not in old_ids:
            mapping = {
                'id': trip_update['id'],
                'trip_id': trip_update['tripUpdate']['trip']['tripId'],
                'start_time': trip_update['tripUpdate']['trip']['startTime'],
                'start_date': trip_update['tripUpdate']['trip']['startDate'],
                'schedule_relationship': trip_update['tripUpdate']['trip']['scheduleRelationship'],
                'route_id': trip_update['tripUpdate']['trip']['routeId'],
                'direction_id': trip_update['tripUpdate']['trip']['directionId'],
                'timestamp': trip_update['tripUpdate']['timestamp']
            }

            # vehicle id is optional (for CANCELED) so we need to check if it exists
            if 'vehicle' in trip_update['tripUpdate']:
                mapping['vehicle_id'] = trip_update['tripUpdate']['vehicle']['id']
            else:
                mapping['vehicle_id'] = None

            to_add.append(mapping)

    if len(to_add) != 0:
        # Insert
        db.session.execute(TripUpdate.__table__.insert().values(to_add))
        db.session.commit()

def update_trip_updates_table(data, old_ids):
    # Optimized update idea from here:
    # https://stackoverflow.com/questions/41870323/sqlalchemy-bulk-update-strategies/41882026#41882026

    # Create a temporary database
    tmp = Table("tmp", db.metadata,
        Column("id", String(64), primary_key=True),
        Column("tripId", String(64)),
        Column("startTime", String(64)),
        Column("startDate", String(64)),
        Column("scheduleRelationship", String(64)),
        Column("routeId", String(64)),
        Column("directionId", Integer),
        Column("timestamp", String(64)),
        Column("vehicleId", String(64)),
        prefixes=["TEMPORARY"]
    )

    tmp.create(bind=db.session.get_bind())
    db.session.commit()

    to_update = []

    # Reformat the data into a dictonary and make a list of them to update
    for i, trip_update in enumerate(data['entity']):
        if trip_update['id'] in old_ids:
            mapping = {
                'id': trip_update['id'],
                'tripId': trip_update['tripUpdate']['trip']['tripId'],
                'startTime': str(trip_update['tripUpdate']['trip']['startTime']),
                'startDate': trip_update['tripUpdate']['trip']['startDate'],
                'scheduleRelationship': trip_update['tripUpdate']['trip']['scheduleRelationship'],
                'routeId': trip_update['tripUpdate']['trip']['routeId'],
                'directionId': trip_update['tripUpdate']['trip']['directionId'],
                'timestamp': trip_update['tripUpdate']['timestamp']
            }

            # vehicle id is optional (for CANCELED) so we need to check if it exists
            if 'vehicle' in trip_update['tripUpdate']:
                mapping['vehicleId'] = trip_update['tripUpdate']['vehicle']['id']
            else:
                mapping['vehicleId'] = None

            to_update.append(mapping)

    # Update the TripUpdate table according to the temporary table
    if len(to_update) != 0:
        db.session.execute(tmp.insert().values(to_update))

        db.session.execute(TripUpdate.__table__
                                     .update()
                                     .values(
                                         id=tmp.c.id,
                                         trip_id=tmp.c.tripId,
                                         start_time=tmp.c.startTime,
                                         start_date=tmp.c.startDate,
                                         schedule_relationship=tmp.c.scheduleRelationship,
                                         route_id=tmp.c.routeId,
                                         direction_id=tmp.c.directionId,
                                         timestamp=tmp.c.timestamp,
                                         vehicle_id=tmp.c.vehicleId
                                     )
                                     .where(TripUpdate.__table__.c.id == tmp.c.id))

    db.session.commit()

def add_new_stop_time_updates(data, old_ids):
    to_add = []

    # Reformat the data into a dictonary and make a list of them to add
    for trip_update in data['entity']:
        if 'stopTimeUpdate' in trip_update['tripUpdate']:

            for stop_time_update in trip_update['tripUpdate']['stopTimeUpdate']:
                id = f"{trip_update['id']}_{stop_time_update['stopSequence']}"

                if id not in old_ids:
                    mapping = {
                        'id': id,
                        'stop_id': stop_time_update['stopId'],
                        'stop_sequence': stop_time_update['stopSequence'],
                        'schedule_relationship': stop_time_update['scheduleRelationship'],
                        'trip_update_id': trip_update['id']
                    }

                    # The arrival and departure times are optional (for CANCELED)
                    # so we need to check if they exist

                    if 'arrival' in stop_time_update:
                        mapping['arrival_time'] = stop_time_update['arrival']['time']
                    else:
                        mapping['arrival_time'] = None

                    if 'departure' in stop_time_update:
                        mapping['departure_time'] = stop_time_update['departure']['time']
                    else:
                        mapping['departure_time'] = None

                    to_add.append(mapping)

    if len(to_add) != 0:
        # Insert
        db.session.execute(StopTimeUpdate.__table__.insert().values(to_add))
        db.session.commit()

def update_stop_time_updates_table(data, old_ids):
    # Optimized update idea from here:
    # https://stackoverflow.com/questions/41870323/sqlalchemy-bulk-update-strategies/41882026#41882026

    # Create a temporary database
    tmp2 = Table("tmp2", db.metadata,
        Column("id", String(64), primary_key=True),
        Column("stopId", String(64)),
        Column("stopSequence", db.Integer),
        Column("arrivalTime", String(64)),
        Column("departureTime", String(64)),
        Column("scheduleRelationship", String(64)),
        Column("tripUpdateId", String(64)),
        prefixes=["TEMPORARY"]
    )

    tmp2.create(bind=db.session.get_bind())
    db.session.commit()

    to_update = []

    # Reformat the data into a dictonary and make a list of them to update
    for trip_update in data['entity']:
        if 'stopTimeUpdate' in trip_update['tripUpdate']:

            for stop_time_update in trip_update['tripUpdate']['stopTimeUpdate']:
                id = f"{trip_update['id']}_{stop_time_update['stopSequence']}"

                if id in old_ids:
                    mapping = {
                        'id': id,
                        'stopId': stop_time_update['stopId'],
                        'stopSequence': stop_time_update['stopSequence'],
                        'scheduleRelationship': stop_time_update['scheduleRelationship'],
                        'tripUpdateId': trip_update['id']
                    }

                    # The arrival and departure times are optional (for CANCELED)
                    # so we need to check if they exist

                    if 'arrival' in stop_time_update:
                        mapping['arrivalTime'] = stop_time_update['arrival']['time']
                    else:
                        mapping['arrivalTime'] = None

                    if 'departure' in stop_time_update:
                        mapping['departureTime'] = stop_time_update['departure']['time']
                    else:
                        mapping['departureTime'] = None

                    to_update.append(mapping)

    # Update the StopTimeUpdate table according to the temporary table
    if len(to_update) != 0:
        db.session.execute(tmp2.insert().values(to_update))

        db.session.execute(StopTimeUpdate.__table__
                                         .update()
                                         .values(
                                             id=tmp2.c.id,
                                             stop_id=tmp2.c.stopId,
                                             stop_sequence=tmp2.c.stopSequence,
                                             schedule_relationship=tmp2.c.scheduleRelationship,
                                             trip_update_id=tmp2.c.tripUpdateId,
                                             arrival_time=tmp2.c.arrivalTime,
                                             departure_time=tmp2.c.departureTime
                                         )
                                         .where(StopTimeUpdate.__table__.c.id == tmp2.c.id))

    db.session.commit()

def parse_feed(data):
    delete_expired_records(data)
    if VERBOSITY >= 1: print('Deleted expired records')

    old_trip_update_ids = [ v[0] for v in db.session.query(TripUpdate.id).all() ]
    old_stop_time_update_ids = [ v[0] for v in db.session.query(StopTimeUpdate.id).all() ]

    add_new_trip_updates(data, old_trip_update_ids)
    if VERBOSITY >= 1: print('Added new entries to TripUpdate')
    update_trip_updates_table(data, old_trip_update_ids)
    if VERBOSITY >= 1: print('Updated old entries in TripUpdate')

    add_new_stop_time_updates(data, old_stop_time_update_ids)
    if VERBOSITY >= 1: print('Added new entries to StopTimeUpdate')
    update_stop_time_updates_table(data, old_stop_time_update_ids)
    if VERBOSITY >= 1: print('Updated old entries in StopTimeUpdate')

def clear_database():
    stmt = StopTimeUpdate.__table__.delete()
    db.session.execute(stmt)

    stmt = TripUpdate.__table__.delete()
    db.session.execute(stmt)

    db.session.commit()

def get_args():
    parser = ArgumentParser(description='VTA Coding Challenge')
    parser.add_argument('-f','--format', help='Format. Only JSON supported', default='json')
    parser.add_argument('-k','--key', help='API key', default='59af72683221a1734f637eae7a7e8d9b')
    parser.add_argument('-v','--verbosity', help='Verbosity level. Only 0, 1, and 2 supported', default='0')
    parser.add_argument('-r','--remote', help='Flag to use remote MySQL database hosted on Amazon RDS instead of default local SQLite database', action='store_true')
    args = vars(parser.parse_args())

    return args

@app.route('/real-time/trip-updates')
def get_trip_updates():
    engine = create_engine('mysql+mysqldb://admin:adminadmin@vta-gtfs-rt.cllzuixyffer.us-east-2.rds.amazonaws.com:3306/vta_gtfs_rt')

    conn = engine.connect()
    result = conn.execute(text('SELECT * FROM trip_updates'))

    output = ''

    for r in result:
        output += str(r)

    return output

if __name__ == '__main__':
    args = get_args()

    format = args['format'].strip().lower()
    key = args['key'].strip()
    verbosity = args['verbosity'].strip()
    remote = args['remote']

    if format != 'json':
        sys.exit('Error: Only JSON format supported. Run again with "--format json".')

    if verbosity.isdigit():
        VERBOSITY = int(args['verbosity'])

    if VERBOSITY >= 2:
        app.config['SQLALCHEMY_ECHO'] = True

    if remote:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqldb://admin:adminadmin@vta-gtfs-rt.cllzuixyffer.us-east-2.rds.amazonaws.com:3306/vta_gtfs_rt'
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///vta-gtfs-rt.sqlite'


    db.init_app(app)

    with app.app_context():

        data = get_json_data(key, format)
        if VERBOSITY >= 1: print('Got API result')

        db.create_all()

        parse_feed(data)
