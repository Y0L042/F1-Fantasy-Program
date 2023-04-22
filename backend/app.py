from flask import Flask, request, jsonify
from your_script import your_etl_function, your_analytics_function
import mysql.connector

def get_db_connection():
    # ... (same as step 5)

@app.route('/etl', methods=['POST'])
def run_etl():
    connection = get_db_connection()
    your_etl_function(connection)
    connection.close()
    return jsonify({'result': 'ETL completed'})

@app.route('/analytics', methods=['GET'])
def run_analytics():
    connection = get_db_connection()
    results = your_analytics_function(connection)
    connection.close()
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
