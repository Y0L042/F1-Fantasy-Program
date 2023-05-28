from datetime import datetime
import json
import fastf1
import pandas as pd
import mysql.connector

import f1stats_database as f1db


import os
import sys

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(MODULE_DIR, 'logs')



# Get the current directory of the script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the 'business_logic' directory to the Python module search path
business_logic_dir = os.path.join(current_dir, 'business_logic')
sys.path.append(business_logic_dir)



season = 0
round_number = 0

# Function to check if the latest race has been loaded
def has_latest_race_been_loaded(season, round_number):
    try:
        with open(os.path.join(LOGS_DIR, "loaded_races.txt"), "r") as file:
            content = file.read()
            return f"{season},{round_number}" in content
    except FileNotFoundError:
        return False



# Function to update the loaded races file
def update_loaded_races(season, round_number):
    with open(os.path.join(LOGS_DIR, "loaded_races.txt"), "a") as file:
        timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        file.write(f"{season},{round_number},{timestamp}\n")


def get_loaded_races_from_database(cursor):
    query = "SELECT CONCAT(season, '-', round_number) as race_id FROM races"
    cursor.execute(query)
    loaded_races = [row[0] for row in cursor.fetchall()]
    return loaded_races


def extract_races_from_fastf1():
    # Enable cache
    # try:
    #     fastf1.Cache.enable_cache('backend/python_business_logic/cache')
    # except Exception as e:
    #     pass

    # Get completed events for a specific season
    global season 
    season = 2023
    schedule = fastf1.get_event_schedule(season)
    completed_events = [schedule.iloc[[event]] for event in schedule['RoundNumber'][1:] if schedule['EventDate'][event] < datetime.now()]

    # Get the latest completed event
    latest_event = completed_events[-1]
    global round_number
    round_number = latest_event['RoundNumber'].values[0]


    # Check if the latest race has been loaded
    if not has_latest_race_been_loaded(season, round_number):
        # Load the latest race data
        session_type = 'R'
        df_session = fastf1.get_session(season, round_number, session_type)
        df_session.load()

        print("Latest race data loaded")

        # Group by driver and get the minimum lap time
        fastest_laps = df_session.laps.groupby('Driver')['LapTime'].min().reset_index()
        
        # Get the driver with the fastest lap time and their lap time
        # fastest_driver = fastest_laps.loc[fastest_laps['LapTime'].idxmin()]
        
        # Create a dataframe with the fastest driver and their lap time
        # df_fastest_lap_driver = pd.DataFrame({'driver': fastest_laps['Driver'], 'fastest_lap': fastest_laps['LapTime']}, index=[0])

        df_fastf1_data = pd.merge(df_session.results[['Abbreviation', 'GridPosition', 'Position', 'Points']], fastest_laps, left_on='Abbreviation', right_on='Driver')

        return df_fastf1_data

    else:
        print("Latest race has already been loaded")
        
        return pd.DataFrame()



def extract_circuit_from_fastf1():
    global season
    season = 2023
    schedule = fastf1.get_event_schedule(season)
    completed_events = [schedule.iloc[[event]] for event in schedule['RoundNumber'][1:] if schedule['EventDate'][event] < datetime.now()]

    latest_event = completed_events[-1]
    global round_number
    round_number = latest_event['RoundNumber'].values[0]

    session_type = 'R'
    df_session = fastf1.get_session(season, round_number, session_type)
    df_session.load()

    # Select the relevant columns from the latest_event DataFrame
    df_circuit_info = latest_event[['RoundNumber', 'Country', 'Location', 'OfficialEventName', 'EventName', 'EventDate', 'EventFormat']].reset_index(drop=True)
    
    # tailor the dataframe

    return df_circuit_info


    

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(os.path.join(LOGS_DIR, "ETL_log.txt"), 'a') as log: 
        log.write(timestamp + ','+str(message)+'\n')







# Transform race results
def transform_race_results(df_extracted_race_results):
    df_transformed_race_results = df_extracted_race_results
    df_transformed_race_results['FastestLap'] = df_extracted_race_results['LapTime'].apply(lambda x: x.total_seconds()) # type: ignore
    df_transformed_race_results = df_transformed_race_results[['Abbreviation', 'GridPosition', 'Position', 'FastestLap', 'Points']]

    missing_values = df_transformed_race_results.isnull().any(axis=1)
    if missing_values.any():
        print("Missing values found:")
        print(df_transformed_race_results[missing_values])

    df_transformed_race_results.fillna(0, inplace=True)

    return df_transformed_race_results


# Transform circuit info
def transform_circuit_info(df_extracted_circuit_info):
    df_transformed_circuit_info = df_extracted_circuit_info[['EventName', 'EventDate', 'RoundNumber', 'Country', 'Location']]

    return df_transformed_circuit_info



# Load data to database
def load_data_to_database(df_transformed_race_results, df_transformed_circuit_info):
    with f1db.connect_to_database() as mydb:
        mydbcursor = mydb.cursor()

        try:
            # Load races table
            for circuit_row in df_transformed_circuit_info.iterrows():
                f1db.insert_into_races(mydbcursor, circuit_row[1])

                # Load race_results table
                for result_row in df_transformed_race_results.iterrows():
                    driver_id_fk = f1db.get_driver_id_fk(mydbcursor, result_row[1]['Abbreviation'])
                    team_id_fk = f1db.get_team_id_by_driver_id_fk(mydbcursor, driver_id_fk)
                    race_id_fk = f1db.get_latest_race_id_fk(mydbcursor)
                    f1db.insert_into_race_results(mydbcursor, result_row[1], driver_id_fk, team_id_fk, race_id_fk)

            mydb.commit()
        except Exception as e:
            print(f"Error occurred while loading data: {e}")
            mydb.rollback()
            raise



def extract_transform_load():
    #-------------------------------------------------------------------------------------------------------
    # Extract
    log('Extract Started')
    df_extracted_race_results = extract_races_from_fastf1()
    if df_extracted_race_results.empty:
        log('Latest Race Already Loaded')
        return

    df_extracted_circuit_info = extract_circuit_from_fastf1()
    log('Extract Finished')

    #-------------------------------------------------------------------------------------------------------
    # Transform
    log('Transform Started')
    df_transformed_race_results = transform_race_results(df_extracted_race_results)
    df_transformed_circuit_info = transform_circuit_info(df_extracted_circuit_info)
    log('Transform Finished')

    #-------------------------------------------------------------------------------------------------------
    # Load
    log('Load Started')
    load_data_to_database(df_transformed_race_results, df_transformed_circuit_info)
    # print('Loading...')
    log('Load Finished')


# extract_transform_load()