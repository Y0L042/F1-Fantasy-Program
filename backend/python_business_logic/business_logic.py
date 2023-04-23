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
            return f"{season},{round_number}" in content
    except FileNotFoundError:
        return False



# Function to update the loaded races file
def update_loaded_races(season, round_number):
    with open("logs/loaded_races.txt", "a") as file:
        timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        file.write(f"{season},{round_number},{timestamp}\n")



def extract_races_from_fastf1():
    # Enable cache
    fastf1.Cache.enable_cache('cache')

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
    with open('logs/ETL_log.txt', 'a') as log: 
        log.write(timestamp + ','+str(message)+'\n')


def extract_transform_load():

    #-------------------------------------------------------------------------------------------------------

    log('Extract Started')

    df_extracted_race_results = extract_races_from_fastf1()
    if df_extracted_race_results.empty:
            log('Latest Race Already Loaded')
            return

    df_extracted_circuit_info = extract_circuit_from_fastf1()

    log('Extract Finished')

    #-------------------------------------------------------------------------------------------------------

    log('Transform Started')

    df_transformed_race_results = df_extracted_race_results
    df_transformed_race_results['FastestLap'] = df_extracted_race_results['LapTime'].apply(lambda x: x.total_seconds()) # type: ignore
    df_transformed_race_results = df_transformed_race_results[['Abbreviation', 'GridPosition', 'Position', 'FastestLap', 'Points']]

    missing_values = df_transformed_race_results.isnull().any(axis=1)
    if missing_values.any():
        print("Missing values found:")
        print(df_transformed_race_results[missing_values])

    df_transformed_race_results.fillna(0, inplace=True)




    # Save the latest race data to a CSV file
    csv_filename = f"transformed_race_csv/transformed_race_data_{season}_round_{round_number}.csv"
    df_transformed_race_results.to_csv(csv_filename, index=False)
    print(f"Latest race data saved to {csv_filename}")

    df_transformed_circuit_info = df_extracted_circuit_info[['EventName', 'EventDate', 'RoundNumber', 'Country', 'Location']]

    log('Transform Finished')

    #-------------------------------------------------------------------------------------------------------

    log('Load Started')

    with f1db.connect_to_database() as mydb:
        mydbcursor = mydb.cursor()

        # Load races table
        for circuit_row in df_transformed_circuit_info.iterrows():
            print('\n')
            print('\n')
            print('\n circuit data:')
            print(circuit_row)
            print('\n')
            print('\n')
            f1db.insert_into_races(mydbcursor, circuit_row[1])

        #     # Load race_results table
            for result_row in df_transformed_race_results.iterrows():
                print('\n')
                print('\n')
                print('\n result data:')
                print('\n')
                print('\n')
                driver_id_fk = f1db.get_driver_id_fk(mydbcursor, result_row[1]['Abbreviation'])
                team_id_fk = f1db.get_team_id_by_driver_id_fk(mydbcursor, driver_id_fk)
                race_id_fk = f1db.get_latest_race_id_fk(mydbcursor)
                f1db.insert_into_race_results(mydbcursor, result_row[1], driver_id_fk, team_id_fk, race_id_fk)


        mydb.commit()

    # Update the loaded races file
    update_loaded_races(season, round_number)

    log('Load Finished')

    #-------------------------------------------------------------------------------------------------------

extract_transform_load()
# print(extract_circuit_from_fastf1())
# print(extract_races_from_fastf1())