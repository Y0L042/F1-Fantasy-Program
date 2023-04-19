from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import glob
import fastf1
from datetime import datetime

def hello_world():
	print('Hello World!')
	return True



def enable_cache():
	fastf1.Cache.enable_cache('cache')



# Get the completed events (race weekends) of the season (year)
def get_completed_events(season, b_print=False):
    # Retrieve the event schedule for the given season using the fastf1 library
    schedule = fastf1.get_event_schedule(season)
    
    # Initialize an empty list to store completed events
    completed_events = []

    # Loop through the round numbers in the event schedule, skipping the first one (index 0)
    for event in schedule['RoundNumber'][1:]:
        # Check if the event date is in the past (i.e., the event is completed)
        if schedule['EventDate'][event] < datetime.now():
            # Append the completed event to the completed_events list
            completed_events.append(schedule.iloc[[event]])

    # Return the list of completed events
    return completed_events
	
	# Commented by GPT4



# Get Amount of completed events of the season (year)
def get_completed_events_count(season):
	count = len(get_completed_events(season))
	return count



# Load the data of a completed session
def get_session_data(season, event_num, session_type):
    df_session = fastf1.get_session(season, event_num, session_type)
    df_session.load()
    return df_session



# Load the data of all the completed sessions
def get_completed_sessions(season, completed_events, session_type):
    # Initialize an empty list to store completed sessions
    completed_sessions = []

    # Loop through the completed_events list, using the enumerate function to keep track of the index
    for i, event in enumerate(completed_events):
        # Get the session data for the current event and append it to the list of completed sessions
        session = get_session_data(season, event['RoundNumber'], session_type)
        completed_sessions.append(session)

    # Return the list of completed sessions
    return completed_sessions

	# commented by GPT3.5


