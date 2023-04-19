import mysql.connector
import requests
import json
import glob
import csv



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



# Insert Functions



# teams table
def insert_into_teams(cursor, data_row):
	try:
		teams_insert_query = '''
			INSERT INTO teams
			(name, points, constructor, nationality)
			VALUES (%s, %s, %s, %s)
		'''
		cursor.execute(teams_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'],
				data_row['column3'],
				data_row['column4'],  
			)
		) #TODO finish it
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into teams: {e}")
		raise



# team_price_history table
def insert_into_team_price_history(cursor, data_row, team_id_fk):
	try:
		team_price_history_insert_query = '''
			INSERT INTO team_price_history
			(price, date, team_id)
			VALUES (%s, %s, %s)
		'''
		cursor.execute(team_price_history_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				team_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into team_price_history: {e}")
		raise



# drivers table
def insert_into_drivers(cursor, data_row, team_id_fk):
	try:
		drivers_insert_query = '''
			INSERT INTO drivers
			(first_name, last_name, country, number, points, team_id)
			VALUES (%s, %s, %s, %s, %s, %s)
		'''
		cursor.execute(drivers_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				data_row['column3'], 
				data_row['column4'], 
				data_row['column5'], 
				team_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into drivers: {e}")
		raise



# driver_price_history table
def insert_into_driver_price_history(cursor, data_row, driver_id_fk):
	try:
		driver_price_history_insert_query = '''
			INSERT INTO driver_price_history
			(price, date, driver_id)
			VALUES (%s, %s, %s)
		'''
		cursor.execute(driver_price_history_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				driver_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into driver_price_history: {e}")
		raise



# circuit table
def insert_into_circuit(cursor, data_row):
	try:
		circuit_insert_query = '''
			INSERT INTO circuit
			(name, country)
			VALUES (%s, %s)
		'''
		cursor.execute(circuit_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into circuit: {e}")
		raise



# race table
def insert_into_race(cursor, data_row, circuit_id_fk):
	try:
		race_insert_query = '''
			INSERT INTO race
			(race_type, date, laps, lap_length, circuit_id)
			VALUES (%s, %s, %s, %s, %s)
		'''
		cursor.execute(race_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				data_row['column3'], 
				data_row['column4'], 
				circuit_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into race: {e}")
		raise



# race_results table
def insert_into_race_results(cursor, data_row, race_id_fk, driver_id_fk, team_id_fk):
	try:
		race_results_insert_query = '''
			INSERT INTO race_results
			(qualifying_position, race_position, fastest_lap_time, race_id, driver_id, team_id)
			VALUES (%s, %s, %s, %s, %s, %s)
		'''
		cursor.execute(race_results_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				data_row['column3'], 
				race_id_fk,
				driver_id_fk,
				team_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into race_results: {e}")
		raise



# weather table
def insert_into_weather(cursor, data_row, race_id_fk):# todo add more weather stuff
	try:
		weather_insert_query = '''
			INSERT INTO weather
			(conditions, temperature, race_id)
			VALUES (%s, %s, %s)
		'''
		cursor.execute(weather_insert_query, 
			(
				data_row['column1'], 
				data_row['column2'], 
				race_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into weather: {e}")
		raise
