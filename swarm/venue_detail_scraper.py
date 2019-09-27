__author__ = “Connor Landy”

""" Program to scrape venue data from the Foursquare API based on 
    a csv of checkin data containing venue id's in the 'id' column. 
    In my case, most interested in the geocoordinates in the response data.

    Returns
    -------
    checkin_data_batch.txt : local file saved containing venue response data
"""

import json, requests, pandas as pd

# Foursquare API call for venue details
url = 'https://api.foursquare.com/v2/venues/'

# parameters for venue api call
params = dict(
  client_id= {your client id here},
  client_secret= {your api key here},
  v=20190820
)

# read in all historical checkins in batches of 500 due to daily limitations on personal application API calls
checkins = pd.read_csv({your local checkin file here})
checkin_locations = checkins['id'][2800:3299]

# for debugging
count = 0

# hit api for every venue, and append the result to checkin_data_batch file as a new row
with open("checkin_data_batch.txt", "a") as f:
  for checkin in checkin_locations:
    try:
      resp = requests.get(url=url + checkin, params=params)
      f.write(resp.text + "\n")
      # for debugging
      print(str(count))
    except:
      print("Failed response for:" + str(count))
    count += 1

print('Done.')
