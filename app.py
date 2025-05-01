from flask import Flask, request, jsonify
import valkey
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

# Initialize Valkey client
valkey_client = valkey.Client(
    host=app.config['VALKEY_HOST'],
    port=app.config['VALKEY_PORT'],
    password=app.config.get('VALKEY_PASSWORD')
)

@app.route('/nearby', methods=['GET'])
def find_nearby():
    """
    Find nearby points based on latitude and longitude.
    
    Query parameters:
    - lat: Latitude (required)
    - lon: Longitude (required)
    - radius: Search radius in meters (optional, default: 1000)
    - unit: Unit of measurement (optional, default: m - meters)
    - count: Maximum number of results (optional, default: 10)
    
    Returns:
    - JSON with nearby location hash keys
    """
    try:
        # Get parameters from request
        lat = request.args.get('lat')
        lon = request.args.get('lon')
        radius = request.args.get('radius', 1000)
        unit = request.args.get('unit', 'm')
        count = request.args.get('count', 10)
        
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
        nearby_points = valkey_client.georadius(
            'points',
            lon,
            lat,
            radius,
            unit=unit,
            count=count,
            withcoord=True,
            withdist=True,
            withhash=True
        )
        
        # Format the response
        results = []
        for point in nearby_points:
            results.append({
                'key': point[0].decode('utf-8'),
                'distance': point[1],
                'coordinates': {
                    'longitude': point[2][0],
                    'latitude': point[2][1]
                },
                'hash': point[3]
            })
        
        return jsonify({
            'count': len(results),
            'results': results
        })
    
    except Exception as e:
        app.logger.error(f"Error processing request: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/', methods=['GET'])
def index():
    """Simple index route to verify the API is running."""
    return jsonify({
        'status': 'ok',
        'message': 'Geospatial API is running'
    })

if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=app.config['PORT'])
