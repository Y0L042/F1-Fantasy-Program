from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import json
# from airflow_docker import business_logic as pbl



app = Flask(__name__)
CORS(app) # Enable CORS for the Flask app -> CORS (Cross-Origin Resource Sharing)




# Create a 'private_config.json' file, and add the details of your database there.
def connect_to_database():
	with open('private_config.json', 'r') as file:
		config_data = json.load(file)


	mydb = mysql.connector.connect(
	host=config_data['db_host'],
	user=config_data['db_user'],
	password=config_data['db_password'],
	database=config_data['db_database']
	)

	return mydb



@app.route('/')
def hello_world():
    return 'Hello, world! Its me, mariao'



@app.route('/db')
def get_teams():
    with connect_to_database() as mydb:
        cursor = mydb.cursor()
        cursor.execute('''
            SELECT * 
            FROM teams
        ''')
        users = cursor.fetchall()
        cursor.close()
        return jsonify(users)


    
if __name__ == '__main__':
    app.run()