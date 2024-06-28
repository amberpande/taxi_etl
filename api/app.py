from flask import Flask, jsonify
from sqlalchemy import create_engine, text
import os

app = Flask(__name__)

db_url = os.getenv('DATABASE_URL')
engine = create_engine(db_url)

@app.route('/aggregates', methods=['GET'])
def get_aggregates():
    with engine.connect() as connection:
        total_records = connection.execute(text("SELECT SUM(trip_count) FROM refined.daily_taxi_stats")).scalar()
        june_17_trips = connection.execute(text("""
            SELECT trip_count FROM refined.daily_taxi_stats
            WHERE trip_date = '2023-06-17'
        """)).scalar()
        longest_trip_day = connection.execute(text("""
            SELECT trip_date FROM refined.daily_taxi_stats
            ORDER BY max_distance DESC LIMIT 1
        """)).scalar()
        distance_stats = connection.execute(text("""
            SELECT 
                AVG(avg_distance) as mean,
                AVG(std_distance) as std_dev,
                MIN(min_distance) as min,
                MAX(max_distance) as max,
                AVG(q1_distance) as q1,
                AVG(median_distance) as median,
                AVG(q3_distance) as q3
            FROM refined.daily_taxi_stats
        """)).fetchone()

    return jsonify({
        'total_records': total_records,
        'june_17_trips': june_17_trips,
        'longest_trip_day': str(longest_trip_day),
        'distance_stats': {
            'mean': float(distance_stats.mean),
            'std_dev': float(distance_stats.std_dev),
            'min': float(distance_stats.min),
            'max': float(distance_stats.max),
            'q1': float(distance_stats.q1),
            'median': float(distance_stats.median),
            'q3': float(distance_stats.q3)
        }
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)