import mysql.connector
import os
import requests
import json
import glob
import csv



# # Create a 'private_config.json' file, and add the details of your database there.
# def connect_to_database():
# 	with open('private_config.json', 'r') as file:
# 		config_data = json.load(file)


# 	mydb = mysql.connector.connect(
# 	host=config_data['db_host'],
# 	user=config_data['db_user'],
# 	password=config_data['db_password'],
# 	database=config_data['db_database']
# 	)

# 	return mydb

def connect_to_database():
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_database = os.environ.get('DB_DATABASE')

    # print(db_host,' ',db_user,' ',db_password, ' ',db_database)

    mydb = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_database
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
				data_row[0], 
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
				data_row[0], 
				team_id_fk
			)
		)
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into drivers: {e}")
		raise


# races table
def insert_into_races(cursor, row_data):
	try:
		race_insert_query = '''
			INSERT INTO races
			(race_name, date, round_number, country, location)
			VALUES (%s, %s, %s, %s, %s)
		'''

		cursor.execute(race_insert_query, 
            (
                row_data['EventName'],
                row_data['EventDate'],
                row_data['RoundNumber'],
                row_data['Country'],
                row_data['Location']
            )
        )
		return cursor.lastrowid
	except Exception as e:
		print(f"Error occurred while inserting into races: {e}")
		raise



def insert_into_race_results(cursor, row_data, driver_id_fk, team_id_fk, race_id_fk):
    try:
        race_results_insert_query = '''
            INSERT INTO race_results
            (driver_shortname, start_pos, finish_pos, fastest_lap, points, drivers_driver_id, teams_team_id, races_race_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.execute(race_results_insert_query,
            (
                row_data['Abbreviation'],
                row_data['GridPosition'],
                row_data['Position'],
                row_data['FastestLap'],
                row_data['Points'],
                driver_id_fk,
                team_id_fk,
                race_id_fk
            )
        )
        return cursor.lastrowid
    except Exception as e:
        print(f"Error occurred while inserting into race_results: {e}")
        raise



def get_driver_id_fk(cursor, driver_abbreviation):
    try:
        select_driver_query = '''
            SELECT driver_id
            FROM drivers
            WHERE driver_shortname = %s
        '''
        cursor.execute(select_driver_query, (driver_abbreviation,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error occurred while selecting from drivers: {e}")
        raise


def get_team_id_by_driver_id_fk(cursor, driver_id):
    try:
        select_team_query = '''
            SELECT teams_team_id
            FROM drivers
            WHERE driver_id = %s
        '''
        cursor.execute(select_team_query, (driver_id,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error occurred while selecting from drivers: {e}")
        raise

def get_latest_race_id_fk(cursor):
    try:
        select_race_query = '''
            SELECT race_id
            FROM races
            ORDER BY race_id DESC
            LIMIT 1
        '''
        cursor.execute(select_race_query)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error occurred while selecting from races: {e}")
        raise
