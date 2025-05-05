from flask import Flask, request, jsonify
import valkey
from config import Config
import datetime

from hf_index.core import get_hfi

app = Flask(__name__)
app.config.from_object(Config)

# Initialize Valkey client
valkey_client = valkey.Redis(
    host=app.config['VALKEY_HOST'],
    port=app.config['VALKEY_PORT'],
    password=app.config.get('VALKEY_PASSWORD'),
    socket_connect_timeout=2.0,  # Add timeout to prevent long blocking
    decode_responses=True        # Automatically decode responses to strings
)

# Function to check database connection
def check_database_connection():
    """Check if the database is available"""
    try:
        # Check connection
        valkey_client.ping()
        
        # Log the number of points in the database
        points_count = valkey_client.zcard('points')
        app.logger.info(f"Connected to database. Found {points_count} points in geospatial index.")
        return True
    except valkey.exceptions.ConnectionError as e:
        app.logger.error(f"Failed to connect to Valkey database: {str(e)}")
        app.logger.error(f"Make sure Valkey is running at {app.config['VALKEY_HOST']}:{app.config['VALKEY_PORT']}")
        return False
    except Exception as e:
        app.logger.error(f"Error initializing sample data: {str(e)}")
        return False

# Check database connection, but continue even if it fails
db_available = check_database_connection()

@app.route('/wxapi/nearby', methods=['GET'])
def find_nearby():
    """
    Find nearby points based on latitude and longitude.
    
    Query parameters:
    - lat: Latitude (required)
    - lon: Longitude (required)
    - radius: Search radius in kilometers (optional, default: 50)
    - unit: Unit of measurement (optional, default: km - kilometers)
    - count: Maximum number of results (optional, default: 1)
    
    Returns:
    - JSON with nearby location hash keys
    """
    try:
        # Check if database is available
        try:
            valkey_client.ping()
        except valkey.exceptions.ConnectionError:
            return jsonify({
                'error': 'Database connection unavailable',
                'message': 'The geospatial database is currently unavailable. Please try again later.'
            }), 503
            
        # Get parameters from request
        lat = request.args.get('lat')
        lon = request.args.get('lon')
        radius = request.args.get('radius', 50)
        unit = request.args.get('unit', 'km')
        count = request.args.get('count', 1)
        
        # Validate required parameters
        if not lat or not lon:
            return jsonify({'error': 'Latitude and longitude are required'}), 400
        
        # Convert parameters to appropriate types
        try:
            lat = float(lat)
            lon = float(lon)
            radius = float(radius)
            count = int(count)
        except ValueError:
            return jsonify({'error': 'Invalid parameter values'}), 400
        
        # Validate unit parameter
        valid_units = ['m', 'km', 'mi', 'ft']
        if unit not in valid_units:
            return jsonify({'error': f'Invalid unit. Must be one of: {", ".join(valid_units)}'}), 400
        
        # Execute GEORADIUS command
        # Based on the sample output, we're just getting the geohash strings
        nearby_points = valkey_client.georadius(
            'points',
            lon,
            lat,
            radius,
            unit=unit,
            count=count,
            sort='ASC'  # Sort by distance, ascending (nearest first)
        )
        
        # Log the search parameters and results for debugging
        app.logger.info(f"Search params: lat={lat}, lon={lon}, radius={radius}{unit}, count={count}")
        app.logger.info(f"Found {len(nearby_points)} points")
        
        # Format the response
        results = []
        for point in nearby_points:
            # Handle the simple string response format
            if isinstance(point, str):
                results.append({'geohash': point})
            # Handle more complex response formats if withcoord, withdist, or withhash were used
            elif isinstance(point, list) or isinstance(point, tuple):
                result = {'geohash': point[0]}
                if len(point) > 1:  # Has distance
                    result['distance'] = point[1]
                if len(point) > 2:  # Has coordinates
                    result['coordinates'] = {
                        'longitude': point[2][0],
                        'latitude': point[2][1]
                    }
                if len(point) > 3:  # Has hash
                    result['hash'] = point[3]
                results.append(result)
            # Fallback for any other format
            else:
                results.append({'geohash': str(point)})
        
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
    Get weather forecast data for a specific interval and geohash.
    
    Query parameters:
    - interval: Forecast interval (e.g., '0h', '1h', '2h') (required)
    - geohash: Geohash location identifier (required)
    
    Returns:
    - JSON with weather forecast data for the specified interval and location
    """
    try:
        # Check if database is available
        try:
            valkey_client.ping()
        except valkey.exceptions.ConnectionError:
            return jsonify({
                'error': 'Database connection unavailable',
                'message': 'The forecast database is currently unavailable. Please try again later.'
            }), 503
            
        # Get parameters from request
        interval = request.args.get('interval')
        geohash = request.args.get('geohash')
        
        # Validate required parameters
        if not interval:
            return jsonify({'error': 'Forecast interval parameter is required'}), 400
        if not geohash:
            return jsonify({'error': 'Geohash parameter is required'}), 400
        
        # Get forecast data from Valkey using the pattern {interval}:{geohash}
        key = f"{interval}:{geohash}"
        forecast_data = valkey_client.hgetall(key)
        
        # Check if data exists for the given parameters
        if not forecast_data:
            return jsonify({'error': f'No forecast data found for interval: {interval}, geohash: {geohash}'}), 404
        
        # Convert numeric values from strings to floats
        for param in forecast_data:
            try:
                forecast_data[param] = float(forecast_data[param])
            except (ValueError, TypeError):
                # Keep as string if not convertible to float
                pass
        
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

@app.route('/wxapi/hfi', methods=['GET'])
def calculate_hfi():
    """
    Calculate Hair Forecast Index (HFI) using forecast data for 8 intervals.
    
    Query parameters:
    - geohash: Geohash location identifier (required)
    - unit: Temperature unit (optional, default: 'K')
    
    Returns:
    - JSON with HFI calculation results for 8 intervals starting from the nearest 6-hour mark
    """
    try:
        # Get parameters from request
        geohash = request.args.get('geohash')
        unit = request.args.get('unit', 'K')
        
        # Validate required parameters
        if not geohash:
            return jsonify({'error': 'Geohash parameter is required'}), 400
        
        # Check if database is available
        try:
            valkey_client.ping()
        except valkey.exceptions.ConnectionError:
            return jsonify({
                'error': 'Database connection unavailable',
                'message': 'The forecast database is currently unavailable. Please try again later.'
            }), 503
        
        # Calculate intervals based on current time
        # Get current UTC time
        now = datetime.datetime.utcnow()
        
        # Find the most recent 00Z, 06Z, 12Z, or 18Z time
        current_hour = now.hour
        base_hour = (current_hour // 6) * 6  # This gives 0, 6, 12, or 18
        
        # Create a datetime for the base time
        base_time = now.replace(hour=base_hour, minute=0, second=0, microsecond=0)
        
        # If we're past the base time, use it as our reference
        # Otherwise, go back to the previous 6-hour mark
        if now < base_time:
            base_time = base_time - datetime.timedelta(hours=6)
        
        # Generate 8 intervals, each 6 hours apart, starting from the base time
        intervals = []
        for i in range(8):
            # Calculate hours from base time
            hours_from_base = i * 6
            interval_str = f"{hours_from_base}h"
            intervals.append(interval_str)
        
        app.logger.info(f"Using intervals: {intervals} based on current UTC time: {now}")
        
        # Store results for each interval
        results = []
        
        # Process each interval
        for interval in intervals:
            # Get forecast data from Valkey using the pattern {interval}:{geohash}
            key = f"{interval}:{geohash}"
            forecast_data = valkey_client.hgetall(key)
            
            # Skip if no data for this interval
            if not forecast_data:
                results.append({
                    'interval': interval,
                    'available': False,
                    'message': f'No forecast data found for interval: {interval}'
                })
                continue
            
            # Convert forecast data values from strings to floats
            for param in forecast_data:
                try:
                    forecast_data[param] = float(forecast_data[param])
                except (ValueError, TypeError):
                    # Keep as string if not convertible to float
                    pass
            
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
            'intervals': results
        })
    
    except Exception as e:
        app.logger.error(f"Error processing Hair Forecast Index request: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/wxapi/', methods=['GET'])
def index():
    """Simple index route to verify the API is running."""
    # Check database connection
    db_status = "connected"
    try:
        valkey_client.ping()
    except Exception:
        db_status = "disconnected"
    
    return jsonify({
        'status': 'ok',
        'message': 'Geospatial API is running',
        'database': db_status
    })

if __name__ == '__main__':
    # We already tried to connect to the database at startup
    # If it failed, we'll retry periodically during runtime
    if not db_available:
        app.logger.warning("Database not available at startup. The API will continue to check for database availability.")
    
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=app.config['PORT'])
