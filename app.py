from flask import Flask, request, jsonify
from config import Config
import datetime
import math

from hf_index.core import get_hfi
from grid import GridStore, GridUnavailable, geohash_encode, geohash_decode

app = Flask(__name__)
app.config.from_object(Config)

# Forecast grid file, reopened automatically when the ingest job replaces it
grid_store = GridStore(app.config['GRID_PATH'])

# Forecast steps are served every 6 hours
INTERVAL_HOURS = 6


# Function to check the grid file
def check_grid():
    """Check if the forecast grid file is available"""
    try:
        grid = grid_store.get()
        app.logger.info(f"Loaded forecast grid. Run {grid.run:%Y-%m-%d %HZ}, steps {grid.steps}, {grid.point_count} points.")
        return True
    except GridUnavailable as e:
        app.logger.error(f"Forecast grid unavailable: {str(e)}")
        app.logger.error(f"Make sure the ingest job has written {app.config['GRID_PATH']}")
        return False

# Check the grid file, but continue even if it is missing
grid_available = check_grid()


def grid_unavailable_response():
    return jsonify({
        'error': 'Database connection unavailable',
        'message': 'The forecast database is currently unavailable. Please try again later.'
    }), 503


def resolve_point(grid):
    """
    Resolve the request's location to a grid point.

    Accepts either a geohash parameter or lat and lon parameters.

    Returns (row, col, geohash, None) on success or (None, None, None, error response).
    """
    geohash = request.args.get('geohash')
    lat = request.args.get('lat')
    lon = request.args.get('lon')

    if geohash:
        try:
            lat, lon = geohash_decode(geohash)
        except ValueError:
            return None, None, None, (jsonify({'error': 'Invalid geohash'}), 400)
    elif lat and lon:
        try:
            lat = float(lat)
            lon = float(lon)
        except ValueError:
            return None, None, None, (jsonify({'error': 'Invalid parameter values'}), 400)
        if not (-90 <= lat <= 90) or not math.isfinite(lon):
            return None, None, None, (jsonify({'error': 'Invalid parameter values'}), 400)
    else:
        return None, None, None, (jsonify({'error': 'Geohash parameter is required'}), 400)

    location = grid.locate(lat, lon)
    if location is None:
        return None, None, None, (jsonify({'error': 'No forecast data found for the specified location'}), 404)

    row, col = location
    if not geohash:
        geohash = geohash_encode(*grid.point(row, col))
    return row, col, geohash, None


def upcoming_steps(grid, count):
    """
    Return the next `count` forecast steps, 6 hours apart, starting from the
    next 6-hour mark after the current time.

    Steps are hours since the model run in the grid file, so a late or missed
    ingest still lines up with the right valid times.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    hours_since_run = (now - grid.run).total_seconds() / 3600.0
    first_step = max(0, math.ceil(hours_since_run / INTERVAL_HOURS)) * INTERVAL_HOURS
    steps = [first_step + (i * INTERVAL_HOURS) for i in range(count)]
    app.logger.info(f"Using steps: {steps} based on current UTC time: {now}, model run: {grid.run}")
    return steps


@app.route('/wxapi/nearby', methods=['GET'])
def find_nearby():
    """
    Find the nearest forecast grid point to a latitude and longitude.

    Query parameters:
    - lat: Latitude (required)
    - lon: Longitude (required)
    - radius, unit, count: Accepted for backward compatibility and ignored.
      The forecast grid is regular, so there is always exactly one nearest point.

    Returns:
    - JSON with the nearest location's geohash
    """
    try:
        try:
            grid = grid_store.get()
        except GridUnavailable:
            return grid_unavailable_response()

        # Get parameters from request
        lat = request.args.get('lat')
        lon = request.args.get('lon')

        # Validate required parameters
        if not lat or not lon:
            return jsonify({'error': 'Latitude and longitude are required'}), 400

        # Convert parameters to appropriate types
        try:
            lat = float(lat)
            lon = float(lon)
        except ValueError:
            return jsonify({'error': 'Invalid parameter values'}), 400
        if not (-90 <= lat <= 90) or not math.isfinite(lon):
            return jsonify({'error': 'Invalid parameter values'}), 400

        results = []
        location = grid.locate(lat, lon)
        if location is not None:
            point_lat, point_lon = grid.point(*location)
            results.append({
                'geohash': geohash_encode(point_lat, point_lon),
                'coordinates': {
                    'longitude': point_lon,
                    'latitude': point_lat
                }
            })

        # Log the search parameters and results for debugging
        app.logger.info(f"Search params: lat={lat}, lon={lon}")
        app.logger.info(f"Found {len(results)} points")

        return jsonify({
            'count': len(results),
            'results': results
        })

    except Exception as e:
        app.logger.error(f"Error processing request: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/wxapi/forecast', methods=['GET'])
def get_forecast():
    """
    Get weather forecast data for a specific interval and location.

    Query parameters:
    - interval: Forecast interval (e.g., '0h', '6h', '12h') (required)
    - geohash: Geohash location identifier (required unless lat and lon are given)
    - lat, lon: Latitude and longitude (alternative to geohash)

    Returns:
    - JSON with weather forecast data for the specified interval and location
    """
    try:
        try:
            grid = grid_store.get()
        except GridUnavailable:
            return grid_unavailable_response()

        # Get parameters from request
        interval = request.args.get('interval')

        # Validate required parameters
        if not interval:
            return jsonify({'error': 'Forecast interval parameter is required'}), 400

        row, col, geohash, error = resolve_point(grid)
        if error:
            return error

        # Intervals are hours since the model run, e.g. '6h'
        try:
            step = int(interval[:-1]) if interval.endswith('h') else int(interval)
        except ValueError:
            step = None
        forecast_data = grid.values(step, row, col) if step is not None else None

        # Check if data exists for the given parameters
        if not forecast_data:
            return jsonify({'error': f'No forecast data found for interval: {interval}, geohash: {geohash}'}), 404

        # Model run date as YYYYMMDD
        forecast_data['dateCode'] = float(grid.run.strftime('%Y%m%d'))

        # Log the request
        app.logger.info(f"Forecast data retrieved for interval: {interval}, geohash: {geohash}")

        # Return the forecast data
        return jsonify({
            'interval': interval,
            'geohash': geohash,
            'forecast': forecast_data
        })

    except Exception as e:
        app.logger.error(f"Error retrieving forecast data: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/wxapi/hfi-detail', methods=['GET'])
def calculate_hfi():
    """
    Calculate Hair Forecast Index (HFI) using forecast data for 8 intervals.

    Query parameters:
    - geohash: Geohash location identifier (required unless lat and lon are given)
    - lat, lon: Latitude and longitude (alternative to geohash)

    Returns:
    - JSON with HFI calculation results for 8 intervals starting from the nearest 6-hour mark
    """
    try:
        try:
            grid = grid_store.get()
        except GridUnavailable:
            return grid_unavailable_response()

        row, col, geohash, error = resolve_point(grid)
        if error:
            return error

        # Store results for each interval
        results = []

        # Process each interval
        for step in upcoming_steps(grid, 8):
            interval = f"{step}h"
            forecast_data = grid.values(step, row, col)

            # Skip if no data for this interval
            if not forecast_data:
                results.append({
                    'interval': interval,
                    'available': False,
                    'message': f'No forecast data found for interval: {interval}'
                })
                continue

            # Extract required parameters for HFI calculation
            try:
                # Map the forecast data fields to the expected parameter names
                t = forecast_data.get('2t')  # Temperature
                d = forecast_data.get('2d')  # Dewpoint
                p = forecast_data.get('tp')  # Precipitation
                u = forecast_data.get('10u')  # Wind U component
                v = forecast_data.get('10v')  # Wind V component

                # Check if all required parameters are available
                if None in (t, d, p, u, v):
                    missing_params = []
                    if t is None: missing_params.append('2t (temperature)')
                    if d is None: missing_params.append('2d (dewpoint)')
                    if p is None: missing_params.append('tp (precipitation)')
                    if u is None: missing_params.append('10u (wind u component)')
                    if v is None: missing_params.append('10v (wind v component)')

                    results.append({
                        'interval': interval,
                        'available': False,
                        'message': f'Missing parameters: {", ".join(missing_params)}'
                    })
                    continue

                # Calculate HFI using the imported get_hfi function
                hfi_result = get_hfi(t, d, p, u, v)

                # Log the calculation
                app.logger.info(f"Hair Forecast Index calculated for interval: {interval}, geohash: {geohash}, result: {hfi_result}")

                # Convert temperature and dewpoint from K to F
                temp_f = (t - 273.15) * 9/5 + 32
                dewpoint_f = (d - 273.15) * 9/5 + 32

                # Calculate wind speed in mph from U and V components
                # Convert from m/s to mph (1 m/s = 2.23694 mph)
                wind_speed_mph = ((u**2 + v**2)**0.5) * 2.23694

                # Add the result for this interval
                results.append({
                    'interval': interval,
                    'valid_time': grid.valid_time(step).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'available': True,
                    'hfi': hfi_result,
                    'temperature_f': round(temp_f, 1),
                    'dewpoint_f': round(dewpoint_f, 1),
                    'wind_mph': round(wind_speed_mph, 1)
                })

            except Exception as e:
                app.logger.error(f"Error calculating Hair Forecast Index for interval {interval}: {str(e)}")
                results.append({
                    'interval': interval,
                    'available': False,
                    'message': f'Error calculating HFI: {str(e)}'
                })

        # Return all interval results
        return jsonify({
            'geohash': geohash,
            'run': grid.run.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'intervals': results
        })

    except Exception as e:
        app.logger.error(f"Error processing Hair Forecast Index request: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/wxapi/hfi-summary', methods=['GET'])
def hfi_summary():
    """
    Calculate summary Hair Forecast Index (HFI) data across 4 intervals (24 hours).

    Query parameters:
    - geohash: Geohash location identifier (required unless lat and lon are given)
    - lat, lon: Latitude and longitude (alternative to geohash)

    Returns:
    - JSON with summary HFI data including high temperature, average wind, and average HFI
    """
    try:
        try:
            grid = grid_store.get()
        except GridUnavailable:
            return grid_unavailable_response()

        row, col, geohash, error = resolve_point(grid)
        if error:
            return error

        # Variables to track summary data
        high_temp_f = float('-inf')
        total_wind_mph = 0
        total_hfi = 0
        valid_intervals = 0

        # Process 4 intervals (24 hours), each 6 hours apart
        for step in upcoming_steps(grid, 4):
            forecast_data = grid.values(step, row, col)

            # Skip if no data for this interval
            if not forecast_data:
                continue

            # Extract required parameters
            t = forecast_data.get('2t')  # Temperature
            d = forecast_data.get('2d')  # Dewpoint
            p = forecast_data.get('tp')  # Precipitation
            u = forecast_data.get('10u')  # Wind U component
            v = forecast_data.get('10v')  # Wind V component

            # Skip if missing any required parameters
            if None in (t, d, p, u, v):
                continue

            # Calculate HFI
            hfi_result = get_hfi(t, d, p, u, v)

            # Convert temperature from K to F
            temp_f = (t - 273.15) * 9/5 + 32

            # Calculate wind speed in mph
            wind_speed_mph = ((u**2 + v**2)**0.5) * 2.23694

            # Update summary data
            high_temp_f = max(high_temp_f, temp_f)
            total_wind_mph += wind_speed_mph
            total_hfi += hfi_result
            valid_intervals += 1

        # Check if we have any valid data
        if valid_intervals == 0:
            return jsonify({
                'error': 'No valid forecast data found for the specified geohash',
                'geohash': geohash
            }), 404

        # Calculate averages
        avg_wind_mph = total_wind_mph / valid_intervals
        avg_hfi = total_hfi / valid_intervals

        # Return summary data
        return jsonify({
            'geohash': geohash,
            'high_temperature_f': round(high_temp_f, 1),
            'average_wind_mph': round(avg_wind_mph, 1),
            'average_hfi': round(avg_hfi, 2),
            'intervals_analyzed': valid_intervals
        })

    except Exception as e:
        app.logger.error(f"Error processing Hair Forecast Index summary request: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/wxapi/', methods=['GET'])
def index():
    """Simple index route to verify the API is running."""
    # Check the forecast grid
    db_status = "connected"
    run = None
    try:
        run = grid_store.get().run.strftime('%Y-%m-%dT%H:%M:%SZ')
    except GridUnavailable:
        db_status = "disconnected"

    return jsonify({
        'status': 'ok',
        'message': 'Geospatial API is running',
        'database': db_status,
        'run': run
    })

if __name__ == '__main__':
    # We already tried to load the grid at startup
    # If it failed, each request retries until the ingest job writes it
    if not grid_available:
        app.logger.warning("Forecast grid not available at startup. The API will continue to check for it.")

    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=app.config['PORT'])
