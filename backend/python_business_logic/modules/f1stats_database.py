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
			(team_name)
			VALUES (%s)
		'''
		cursor.execute(teams_insert_query, 
			(
				data_row.iloc[:,0], 
			)
		) #TODO finish it
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into teams: {e}")
		raise



# drivers table
def insert_into_drivers(cursor, data_row, team_id_fk):
	try:
		drivers_insert_query = '''
			INSERT INTO drivers
			(driver_shortname, teams_team_id)
			VALUES (%s, %s)
		'''
		cursor.execute(drivers_insert_query, 
			(
				data_row.iloc[:,0], 
				team_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into drivers: {e}")
		raise


# race table
def insert_into_race(cursor, data_row, circuit_id_fk):
	try:
		race_insert_query = '''
			INSERT INTO race
			(race_name, date, location)
			VALUES (%s, %s, %s)
		'''
		cursor.execute(race_insert_query, 
			(
				data_row.iloc[:,0], 
				data_row.iloc[:,1], 
				data_row.iloc[:,2],
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
			(driver_shortname, start_pos, finish_pos, fastest_lap, drivers_driver_id, teams_team_id, races_race_id)
			VALUES (%s, %s, %s, %s, %s, %s)
		'''
		cursor.execute(race_results_insert_query, 
			(
				data_row.iloc[:,0], 
				data_row.iloc[:,1], 
				data_row.iloc[:,2], 
				driver_id_fk,
				team_id_fk,
				race_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into race_results: {e}")
		raise

