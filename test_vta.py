import unittest
import json

from vta import app, StopTimeUpdate, TripUpdate

class EndpointTest(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.response = self.client.get('/real-time/trip-updates')
        self.data = json.loads(self.response.text)

    def test_response_status_code(self):
        """Check that we get OK response"""
        self.assertTrue(self.response.status_code == 200)

    def test_type(self):
        """Check that response is a list"""

        self.assertTrue(isinstance(self.data, list))

    def test_keys(self):
        """Check that each item in response has an 'id' and 'tripUpdate' key"""

        for item in self.data:
            self.assertTrue('id' in item)
            self.assertTrue('tripUpdate' in item)

class StopTimeUpdateTest(unittest.TestCase):
    def test_dict_format(self):
        """Check that the dict_format() function of StopTimeUpdate does what's expected"""

        stop_time_update = StopTimeUpdate(
            id = '3279419_165_77340_47',
            stop_id = '422',
            stop_sequence = '47',
            schedule_relationship = 'SCHEDULED',
            trip_update_id = '3279419_165_77340',
            arrival_time = '1682313086'
        )

        dict_form = stop_time_update.dict_format()

        self.assertTrue(dict_form['arrival']['time'] == '1682313086')
        self.assertTrue(dict_form['scheduleRelationship'] == 'SCHEDULED')
        self.assertTrue(dict_form['stopId'] == '422')
        self.assertTrue(dict_form['stopSequence'] == '47')
        self.assertTrue('departure' not in dict_form)
        self.assertTrue('uncertainty' not in dict_form['arrival'])

class TripUpdateTest(unittest.TestCase):
    def test_dict_format(self):
        """Check that the dict_format() function of TripUpdate does what's expected"""

        trip_update = TripUpdate(
            id = '3279419_165_77340',
            trip_id = '3279419',
            start_time = '21:29:00',
            start_date = '20230423',
            schedule_relationship = 'SCHEDULED',
            route_id = '23',
            direction_id = 0,
            timestamp = '1682313447',
            vehicle_id = '165'
        )

        dict_form = trip_update.dict_format()

        self.assertTrue(dict_form['id'] == '3279419_165_77340')
        self.assertTrue(dict_form['tripUpdate']['trip']['tripId'] == '3279419')
        self.assertTrue(dict_form['tripUpdate']['trip']['startTime'] == '21:29:00')
        self.assertTrue(dict_form['tripUpdate']['trip']['startDate'] == '20230423')
        self.assertTrue(dict_form['tripUpdate']['trip']['scheduleRelationship'] == 'SCHEDULED')
        self.assertTrue(dict_form['tripUpdate']['trip']['routeId'] == '23')
        self.assertTrue(dict_form['tripUpdate']['trip']['directionId'] == 0)
        self.assertTrue(dict_form['tripUpdate']['timestamp'] == '1682313447')
        self.assertTrue(dict_form['tripUpdate']['vehicle']['id'] == '165')
        self.assertTrue('stopTimeUpdate' not in dict_form['tripUpdate'])

if __name__ == '__main__':
    unittest.main()
