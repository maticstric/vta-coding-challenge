import json
import sys
import time

from argparse import ArgumentParser
import requests
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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
    arrival_uncertainty = db.Column(db.String(64))
    departure_uncertainty = db.Column(db.String(64))
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

        if self.arrival_uncertainty:
            d['arrival'] = { 'uncertainty': self.arrival_uncertainty }

        if self.departure_uncertainty:
            d['departure'] = { 'uncertainty': self.departure_uncertainty }

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
    tmp_tu = Table('tmp_tu', db.metadata,
        Column('id', String(64), primary_key=True),
        Column('tripId', String(64)),
        Column('startTime', String(64)),
        Column('startDate', String(64)),
        Column('scheduleRelationship', String(64)),
        Column('routeId', String(64)),
        Column('directionId', Integer),
        Column('timestamp', String(64)),
        Column('vehicleId', String(64)),
        prefixes=['TEMPORARY']
    )

    tmp_tu.create(bind=db.session.get_bind())
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
        db.session.execute(tmp_tu.insert().values(to_update))

        db.session.execute(TripUpdate.__table__
                                     .update()
                                     .values(
                                         id=tmp_tu.c.id,
                                         trip_id=tmp_tu.c.tripId,
                                         start_time=tmp_tu.c.startTime,
                                         start_date=tmp_tu.c.startDate,
                                         schedule_relationship=tmp_tu.c.scheduleRelationship,
                                         route_id=tmp_tu.c.routeId,
                                         direction_id=tmp_tu.c.directionId,
                                         timestamp=tmp_tu.c.timestamp,
                                         vehicle_id=tmp_tu.c.vehicleId
                                     )
                                     .where(TripUpdate.__table__.c.id == tmp_tu.c.id))

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

                    # The arrival and departure times/uncertainties are optional (for CANCELED)
                    # so we need to check if they exist

                    if 'arrival' in stop_time_update:
                        mapping['arrival_time'] = stop_time_update['arrival']['time']

                        if 'uncertainty' in stop_time_update['arrival']:
                            mapping['arrival_uncertainty'] = stop_time_update['arrival']['uncertainty']
                        else:
                            mapping['arrival_uncertainty'] = None
                    else:
                        mapping['arrival_time'] = None
                        mapping['arrival_uncertainty'] = None

                    if 'departure' in stop_time_update:
                        mapping['departure_time'] = stop_time_update['departure']['time']

                        if 'uncertainty' in stop_time_update['departure']:
                            mapping['departure_uncertainty'] = stop_time_update['departure']['uncertainty']
                        else:
                            mapping['departure_uncertainty'] = None
                    else:
                        mapping['departure_time'] = None
                        mapping['departure_uncertainty'] = None

                    to_add.append(mapping)

    if len(to_add) != 0:
        # Insert
        db.session.execute(StopTimeUpdate.__table__.insert().values(to_add))
        db.session.commit()

def update_stop_time_updates_table(data, old_ids):
    # Optimized update idea from here:
    # https://stackoverflow.com/questions/41870323/sqlalchemy-bulk-update-strategies/41882026#41882026

    # Create a temporary database
    tmp_stu = Table('tmp_stu', db.metadata,
        Column('id', String(64), primary_key=True),
        Column('stopId', String(64)),
        Column('stopSequence', db.Integer),
        Column('arrivalTime', String(64)),
        Column('departureTime', String(64)),
        Column('arrivalUncertainty', String(64)),
        Column('departureUncertainty', String(64)),
        Column('scheduleRelationship', String(64)),
        Column('tripUpdateId', String(64)),
        prefixes=['TEMPORARY']
    )

    tmp_stu.create(bind=db.session.get_bind())
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

                    # The arrival and departure times/uncertainties are optional (for CANCELED)
                    # so we need to check if they exist

                    if 'arrival' in stop_time_update:
                        mapping['arrivalTime'] = stop_time_update['arrival']['time']

                        if 'uncertainty' in stop_time_update['arrival']:
                            mapping['arrivalUncertainty'] = stop_time_update['arrival']['uncertainty']
                        else:
                            mapping['arrivalUncertainty'] = None
                    else:
                        mapping['arrivalTime'] = None
                        mapping['arrivalUncertainty'] = None

                    if 'departure' in stop_time_update:
                        mapping['departureTime'] = stop_time_update['departure']['time']

                        if 'uncertainty' in stop_time_update['departure']:
                            mapping['departureUncertainty'] = stop_time_update['departure']['uncertainty']
                        else:
                            mapping['departureUncertainty'] = None
                    else:
                        mapping['departureTime'] = None
                        mapping['departureUncertainty'] = None

                    to_update.append(mapping)

    # Update the StopTimeUpdate table according to the temporary table
    if len(to_update) != 0:
        db.session.execute(tmp_stu.insert().values(to_update))

        db.session.execute(StopTimeUpdate.__table__
                                         .update()
                                         .values(
                                             id=tmp_stu.c.id,
                                             stop_id=tmp_stu.c.stopId,
                                             stop_sequence=tmp_stu.c.stopSequence,
                                             schedule_relationship=tmp_stu.c.scheduleRelationship,
                                             trip_update_id=tmp_stu.c.tripUpdateId,
                                             arrival_time=tmp_stu.c.arrivalTime,
                                             departure_time=tmp_stu.c.departureTime,
                                             arrival_uncertainty=tmp_stu.c.arrivalUncertainty,
                                             departure_uncertainty=tmp_stu.c.departureUncertainty
                                         )
                                         .where(StopTimeUpdate.__table__.c.id == tmp_stu.c.id))

    db.session.commit()

def parse_feed(data):
    if VERBOSITY >= 1: print('Deleting expired records...', end=' ', flush=True)
    start_time = time.time()
    delete_expired_records(data)
    if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

    old_trip_update_ids = [ v[0] for v in db.session.query(TripUpdate.id).all() ]
    old_stop_time_update_ids = [ v[0] for v in db.session.query(StopTimeUpdate.id).all() ]

    if VERBOSITY >= 1: print('Adding new entries to TripUpdate...', end=' ', flush=True)
    start_time = time.time()
    add_new_trip_updates(data, old_trip_update_ids)
    if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

    if VERBOSITY >= 1: print('Updating old entries in TripUpdate...', end=' ', flush=True)
    start_time = time.time()
    update_trip_updates_table(data, old_trip_update_ids)
    if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

    if VERBOSITY >= 1: print('Adding new entries to StopTimeUpdate...', end=' ', flush=True)
    start_time = time.time()
    add_new_stop_time_updates(data, old_stop_time_update_ids)
    if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

    if VERBOSITY >= 1: print('Updating old entries in StopTimeUpdate...', end=' ', flush=True)
    start_time = time.time()
    update_stop_time_updates_table(data, old_stop_time_update_ids)
    if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

def clear_database():
    stmt = StopTimeUpdate.__table__.delete()
    db.session.execute(stmt)

    stmt = TripUpdate.__table__.delete()
    db.session.execute(stmt)

    db.drop_all(bind=db.session.get_bind())

    db.session.commit()

def get_args():
    parser = ArgumentParser(description='VTA Coding Challenge')
    parser.add_argument('-f','--format', help='Format. Only JSON supported. JSON is default', default='json')
    parser.add_argument('-k','--key', help='API key. 59af72683221a1734f637eae7a7e8d9b is default', default='59af72683221a1734f637eae7a7e8d9b')
    parser.add_argument('-v','--verbosity', help='Verbosity level. Only 0, 1, and 2 supported. Level 1 is default with a few custom messages. Level 2 shows all SQL commands being run', default='1')
    parser.add_argument('-r','--remote', help='Flag to use remote MySQL database hosted on Amazon RDS instead of default local SQLite database. Use this option to update the remote database so you can get up-to-date data from the endpoint', action='store_true')
    args = vars(parser.parse_args())

    return args

@app.route('/real-time/trip-updates')
def get_trip_updates():
    engine = create_engine('mysql+mysqldb://admin:adminadmin@vta.cllzuixyffer.us-east-2.rds.amazonaws.com:3306/vta')

    Session = sessionmaker(engine)

    num_entries = request.args.get('num_entries')
    if num_entries == None: num_entries = 100 # Default to 100

    with Session() as session:
        output = []

        trip_updates = session.query(TripUpdate).limit(num_entries).all()
        stop_time_updates = session.query(StopTimeUpdate).all()

        for trip_update in trip_updates:
            d = trip_update.dict_format()

            related_stu = [ stu for stu in stop_time_updates if stu.trip_update_id == trip_update.id ]

            if len(related_stu) > 0: d['tripUpdate']['stopTimeUpdate'] = []

            for stu in related_stu:
                d['tripUpdate']['stopTimeUpdate'].append(stu.dict_format())

            output.append(d)

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
        app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqldb://admin:adminadmin@vta.cllzuixyffer.us-east-2.rds.amazonaws.com:3306/vta'
    else:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///vta-gtfs-rt.sqlite'


    db.init_app(app)

    with app.app_context():
        if VERBOSITY >= 1: print('Getting API result...', end=' ', flush=True)
        start_time = time.time()
        data = get_json_data(key, format)
        if VERBOSITY >= 1: print('DONE (in ' + str(round(time.time() - start_time, 4)) + 's)')

        db.create_all()

        parse_feed(data)
