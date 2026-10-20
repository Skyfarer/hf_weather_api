import datetime
import json
import os
import struct
import tempfile
import unittest

from grid import MAGIC, DATA_ALIGN, Grid, GridStore, GridUnavailable, geohash_encode, geohash_decode

PARAMS = ['2t', '2d', '10u', '10v', 'tp']
NLAT, NLON = 5, 8  # 20N..0N, 180W..135E in 5 and 45 degree spacing


def write_grid(path, run, steps):
    """Write a small grid file where every value encodes its own position."""
    header = json.dumps({
        'run': run.strftime('%Y-%m-%dT%H:%M:%SZ'), 'steps': steps, 'params': PARAMS,
        'lat0': 20.0, 'lon0': -180.0, 'dlat': -5.0, 'dlon': 45.0,
        'nlat': NLAT, 'nlon': NLON, 'dtype': '<f4',
    }).encode('utf-8')
    prefix_len = len(MAGIC) + 4 + len(header)
    tmp_path = path + '.tmp'
    with open(tmp_path, 'wb') as f:
        f.write(MAGIC + struct.pack('<I', len(header)) + header)
        f.write(b'\0' * (-prefix_len % DATA_ALIGN))
        for step in steps:
            for p in range(len(PARAMS)):
                for row in range(NLAT):
                    for col in range(NLON):
                        f.write(struct.pack('<f', cell_value(step, p, row, col)))
    os.replace(tmp_path, path)


def cell_value(step, p, row, col):
    # Temperature and dewpoint in Kelvin, everything else small
    base = 290.0 if p < 2 else 0.0
    return base + step / 100.0 + p + row / 10.0 + col / 100.0


class GeohashTest(unittest.TestCase):
    def test_encode_known_value(self):
        self.assertEqual(geohash_encode(57.64911, 10.40744), 'u4pruydqq')

    def test_round_trip(self):
        lat, lon = geohash_decode(geohash_encode(35.5, -85.0))
        self.assertAlmostEqual(lat, 35.5, places=4)
        self.assertAlmostEqual(lon, -85.0, places=4)

    def test_invalid_character(self):
        with self.assertRaises(ValueError):
            geohash_decode('abc')  # 'a' is not in the geohash alphabet


class GridTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, 'forecast.grid')
        self.run = datetime.datetime(2025, 4, 10, tzinfo=datetime.timezone.utc)
        write_grid(self.path, self.run, [0, 6])

    def tearDown(self):
        self.dir.cleanup()

    def test_header(self):
        grid = Grid(self.path)
        self.assertEqual(grid.run, self.run)
        self.assertEqual(grid.steps, [0, 6])
        self.assertEqual(grid.point_count, NLAT * NLON)
        self.assertEqual(grid.valid_time(6), self.run + datetime.timedelta(hours=6))

    def test_locate_snaps_to_nearest_point(self):
        grid = Grid(self.path)
        self.assertEqual(grid.locate(20, -180), (0, 0))
        self.assertEqual(grid.locate(11, -40), (2, 3))
        self.assertEqual(grid.point(2, 3), (10.0, -45.0))

    def test_locate_wraps_longitude(self):
        grid = Grid(self.path)
        self.assertEqual(grid.locate(20, 180), (0, 0))
        self.assertEqual(grid.locate(20, 170), (0, 0))

    def test_locate_outside_latitude_range(self):
        grid = Grid(self.path)
        self.assertIsNone(grid.locate(30, 0))
        self.assertIsNone(grid.locate(-10, 0))

    def test_values(self):
        grid = Grid(self.path)
        values = grid.values(6, 2, 3)
        for p, param in enumerate(PARAMS):
            self.assertAlmostEqual(values[param], cell_value(6, p, 2, 3), places=3)
        self.assertIsNone(grid.values(12, 2, 3))

    def test_truncated_file_is_rejected(self):
        with open(self.path, 'r+b') as f:
            f.truncate(os.path.getsize(self.path) - 4)
        with self.assertRaises(GridUnavailable):
            Grid(self.path)

    def test_store_missing_file(self):
        with self.assertRaises(GridUnavailable):
            GridStore(os.path.join(self.dir.name, 'missing.grid')).get()

    def test_store_reloads_replaced_file(self):
        store = GridStore(self.path)
        self.assertEqual(store.get().steps, [0, 6])
        write_grid(self.path, self.run + datetime.timedelta(days=1), [0, 6, 12])
        self.assertEqual(store.get().steps, [0, 6, 12])
        self.assertEqual(store.get().run, self.run + datetime.timedelta(days=1))


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.TemporaryDirectory()
        cls.path = os.path.join(cls.dir.name, 'forecast.grid')
        os.environ['HF_GRID_PATH'] = cls.path
        import app as app_module
        cls.app_module = app_module
        app_module.grid_store.path = cls.path
        cls.client = app_module.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.dir.cleanup()

    def write(self, hours_ago, steps):
        """Write a grid whose model run was `hours_ago` hours before now."""
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        write_grid(self.path, now - datetime.timedelta(hours=hours_ago), steps)

    def test_unavailable_without_grid(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        self.assertEqual(self.client.get('/wxapi/nearby?lat=10&lon=-45').status_code, 503)
        self.assertEqual(self.client.get('/wxapi/').get_json()['database'], 'disconnected')

    def test_nearby_returns_grid_point_geohash(self):
        self.write(1, [0, 6])
        data = self.client.get('/wxapi/nearby?lat=11&lon=-40').get_json()
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['geohash'], geohash_encode(10.0, -45.0))
        self.assertEqual(data['results'][0]['coordinates'], {'latitude': 10.0, 'longitude': -45.0})

    def test_nearby_outside_grid_is_empty(self):
        self.write(1, [0, 6])
        self.assertEqual(self.client.get('/wxapi/nearby?lat=60&lon=0').get_json(), {'count': 0, 'results': []})

    def test_nearby_validation(self):
        self.write(1, [0, 6])
        self.assertEqual(self.client.get('/wxapi/nearby?lat=10').status_code, 400)
        self.assertEqual(self.client.get('/wxapi/nearby?lat=x&lon=1').status_code, 400)
        self.assertEqual(self.client.get('/wxapi/nearby?lat=nan&lon=1').status_code, 400)

    def test_forecast_by_geohash_and_latlon(self):
        self.write(1, [0, 6])
        geohash = geohash_encode(10.0, -45.0)
        by_hash = self.client.get(f'/wxapi/forecast?interval=6h&geohash={geohash}').get_json()
        by_coord = self.client.get('/wxapi/forecast?interval=6h&lat=10&lon=-45').get_json()
        self.assertEqual(by_hash, by_coord)
        self.assertAlmostEqual(by_hash['forecast']['2t'], cell_value(6, 0, 2, 3), places=3)
        self.assertEqual(set(by_hash['forecast']), set(PARAMS) | {'dateCode'})
        self.assertEqual(self.client.get(f'/wxapi/forecast?interval=12h&geohash={geohash}').status_code, 404)
        self.assertEqual(self.client.get(f'/wxapi/forecast?interval=bogus&geohash={geohash}').status_code, 404)
        self.assertEqual(self.client.get('/wxapi/forecast?interval=6h').status_code, 400)

    def test_detail_starts_at_next_six_hour_mark(self):
        self.write(1, list(range(0, 67, 6)))
        data = self.client.get('/wxapi/hfi-detail?lat=10&lon=-45').get_json()
        self.assertEqual([i['interval'] for i in data['intervals']], [f'{h}h' for h in range(6, 54, 6)])
        self.assertTrue(all(i['available'] for i in data['intervals']))
        self.assertIn('hfi', data['intervals'][0])

    def test_detail_follows_a_stale_run(self):
        # A run from 25 hours ago: the next 6-hour mark is its 30h step
        self.write(25, list(range(0, 67, 6)))
        data = self.client.get('/wxapi/hfi-detail?lat=10&lon=-45').get_json()
        self.assertEqual(data['intervals'][0]['interval'], '30h')
        self.assertEqual([i['available'] for i in data['intervals']], [True] * 7 + [False])

    def test_summary(self):
        self.write(1, list(range(0, 67, 6)))
        data = self.client.get('/wxapi/hfi-summary?lat=10&lon=-45').get_json()
        self.assertEqual(data['intervals_analyzed'], 4)
        self.assertEqual(data['geohash'], geohash_encode(10.0, -45.0))

    def test_summary_with_expired_run(self):
        self.write(100, list(range(0, 67, 6)))
        self.assertEqual(self.client.get('/wxapi/hfi-summary?lat=10&lon=-45').status_code, 404)

    def test_bad_geohash(self):
        self.write(1, [0, 6])
        self.assertEqual(self.client.get('/wxapi/hfi-summary?geohash=!!!').status_code, 400)
        self.assertEqual(self.client.get('/wxapi/hfi-summary').status_code, 400)


if __name__ == '__main__':
    unittest.main()
