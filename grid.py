"""
Reader for the forecast grid file written by the ingest job (build_grid.py).

File layout:
    bytes 0-7    magic b"HFGRID1\\n"
    bytes 8-11   header length N (uint32, little endian)
    bytes 12-    N bytes of UTF-8 JSON header
    padding      zero bytes up to the next multiple of 4096
    data         float32 little endian, C order [step][param][row][col]

The file is memory-mapped, so only the pages a request touches are read.
"""
import datetime
import json
import math
import mmap
import os
import struct
import threading

MAGIC = b"HFGRID1\n"
DATA_ALIGN = 4096

GEOHASH_BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz'
GEOHASH_PRECISION = 9


class GridUnavailable(Exception):
    """Raised when the grid file is missing or unreadable."""


def geohash_encode(lat, lon, precision=GEOHASH_PRECISION):
    """Encode a latitude/longitude as a geohash string."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    chars = []
    bits = 0
    value = 0
    even = True  # Even bits encode longitude
    while len(chars) < precision:
        rng, coord = (lon_range, lon) if even else (lat_range, lat)
        mid = (rng[0] + rng[1]) / 2
        if coord >= mid:
            value = (value << 1) | 1
            rng[0] = mid
        else:
            value <<= 1
            rng[1] = mid
        even = not even
        bits += 1
        if bits == 5:
            chars.append(GEOHASH_BASE32[value])
            bits = 0
            value = 0
    return ''.join(chars)


def geohash_decode(geohash):
    """Decode a geohash string to the (lat, lon) of its cell centre."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    even = True
    for char in geohash:
        index = GEOHASH_BASE32.find(char)
        if index < 0:
            raise ValueError(f"Invalid geohash character: {char}")
        for shift in range(4, -1, -1):
            rng = lon_range if even else lat_range
            mid = (rng[0] + rng[1]) / 2
            if (index >> shift) & 1:
                rng[0] = mid
            else:
                rng[1] = mid
            even = not even
    return (lat_range[0] + lat_range[1]) / 2, (lon_range[0] + lon_range[1]) / 2


class Grid:
    """One memory-mapped forecast grid file."""

    def __init__(self, path):
        with open(path, 'rb') as f:
            self._map = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        if self._map[:len(MAGIC)] != MAGIC:
            raise GridUnavailable(f"{path} is not a forecast grid file")

        (header_len,) = struct.unpack_from('<I', self._map, len(MAGIC))
        header_start = len(MAGIC) + 4
        header = json.loads(self._map[header_start:header_start + header_len].decode('utf-8'))

        if header['dtype'] != '<f4':
            raise GridUnavailable(f"Unsupported grid dtype: {header['dtype']}")

        self.run = datetime.datetime.strptime(header['run'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
        self.steps = header['steps']
        self.params = header['params']
        self.lat0 = header['lat0']
        self.lon0 = header['lon0']
        self.dlat = header['dlat']
        self.dlon = header['dlon']
        self.nlat = header['nlat']
        self.nlon = header['nlon']

        prefix_len = header_start + header_len
        self._data_offset = prefix_len + (-prefix_len % DATA_ALIGN)

        expected = self._data_offset + len(self.steps) * len(self.params) * self.nlat * self.nlon * 4
        if len(self._map) != expected:
            raise GridUnavailable(f"{path} is {len(self._map)} bytes, expected {expected}")

    @property
    def point_count(self):
        return self.nlat * self.nlon

    def locate(self, lat, lon):
        """Return the (row, col) of the grid point nearest to lat/lon, or None if outside the grid."""
        row = int(round((lat - self.lat0) / self.dlat))
        if row < 0 or row >= self.nlat:
            return None
        # Longitude wraps around the globe
        col = int(round((lon - self.lon0) / self.dlon)) % self.nlon
        return row, col

    def point(self, row, col):
        """Return the (lat, lon) of a grid point."""
        return self.lat0 + row * self.dlat, self.lon0 + col * self.dlon

    def valid_time(self, step):
        """Return the UTC time a forecast step is valid for."""
        return self.run + datetime.timedelta(hours=step)

    def values(self, step, row, col):
        """
        Return {param: value} for one step at one grid point.

        Returns None if the step is not in the file. Missing values are None.
        """
        if step not in self.steps:
            return None
        plane = self.nlat * self.nlon
        base = self._data_offset + self.steps.index(step) * len(self.params) * plane * 4
        cell = (row * self.nlon + col) * 4
        result = {}
        for i, param in enumerate(self.params):
            (value,) = struct.unpack_from('<f', self._map, base + i * plane * 4 + cell)
            result[param] = None if math.isnan(value) else value
        return result


class GridStore:
    """Opens the grid file and reopens it when the ingest job replaces it."""

    def __init__(self, path):
        self.path = path
        self._grid = None
        self._signature = None
        self._lock = threading.Lock()

    def get(self):
        """Return the current Grid, raising GridUnavailable if there is none."""
        try:
            stat = os.stat(self.path)
        except OSError as e:
            raise GridUnavailable(f"Cannot read grid file {self.path}: {e}")

        signature = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        with self._lock:
            if signature != self._signature:
                try:
                    grid = Grid(self.path)
                except GridUnavailable:
                    raise
                except Exception as e:
                    raise GridUnavailable(f"Cannot read grid file {self.path}: {e}")
                # The previous map is released once requests using it finish
                self._grid = grid
                self._signature = signature
            return self._grid
