from datetime import datetime
import json
import fastf1
import pandas as pd
import mysql.connector

import modules.f1stats_database as f1db

season = 0
round_number = 0

# Function to check if the latest race has been loaded
def has_latest_race_been_loaded(season, round_number):
    try:
        with open("logs/loaded_races.txt", "r") as file:
            content = file.read()
            return f"{season}-{round_number}" in content
    except FileNotFoundError:
        return False

# Function to update the loaded races file
def update_loaded_races(season, round_number):
    with open("logs/loaded_races.txt", "a") as file:
        file.write(f"{season}-{round_number}\n")


def extract_from_fastf1():
    # Enable cache
    # fastf1.Cache.enable_cache('cache')

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
        fastest_driver = fastest_laps.loc[fastest_laps['LapTime'].idxmin()]
        
        # Create a dataframe with the fastest driver and their lap time
        df_fastest_lap_driver = pd.DataFrame({'driver': fastest_driver['Driver'], 'fastest_lap': fastest_driver['LapTime']}, index=[0])

        df_fastf1_data = pd.merge(df_session.results[['Abbreviation', 'GridPosition', 'Position', 'Points']], df_fastest_lap_driver, left_on='Abbreviation', right_on='driver')

        return df_fastf1_data

    else:
        print("Latest race has already been loaded")
        
        return pd.DataFrame()


def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('logs/ETL_log.txt', 'a') as log: 
        log.write(timestamp + ','+str(message)+'\n')


def extract_transform_load():

    #-------------------------------------------------------------------------------------------------------

    df_extracted_race_results = extract_from_fastf1()
    if df_extracted_race_results.empty:
            log('Latest Race Already Loaded')
            return

    log('Extract Started')

    # # Save the latest race data to a CSV file
    # csv_filename = f"extracted_race_data_{season}_round_{round_number}.csv"
    # df_extracted_race_results.to_csv(csv_filename, index=False)
    # print(f"Latest race data saved to {csv_filename}")

    log('Extract Finished')

    #-------------------------------------------------------------------------------------------------------

    log('Transform Started')

    df_transformed_race_results = df_extracted_race_results
    df_transformed_race_results['fastest_lap'] = df_extracted_race_results['fastest_lap'].apply(lambda x: x.total_seconds()) # type: ignore

    # Save the latest race data to a CSV file
    csv_filename = f"transformed_race_csv/transformed_race_data_{season}_round_{round_number}.csv"
    df_transformed_race_results.to_csv(csv_filename, index=False)
    print(f"Latest race data saved to {csv_filename}")

    log('Transform Finished')

    #-------------------------------------------------------------------------------------------------------

    log('Load Started')

    mydb = f1db.connect_to_database()
    mydbcursor = mydb.cursor()


    # Update the loaded races file
    update_loaded_races(season, round_number)

    log('Load Finished')

    #-------------------------------------------------------------------------------------------------------

extract_transform_load()