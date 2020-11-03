#!/usr/bin/env python3

#
# This script will walk through all the tweet id files and
# hydrate them with twarc. The line oriented JSON files will
# be placed right next to each tweet id file.
#
# Note: you will need to install twarc, tqdm, and run twarc configure
# from the command line to tell it your Twitter API keys.
#

import gzip
import json

from tqdm import tqdm
from twarc import Twarc
from pathlib import Path

data_dirs = []#['2020-01', '2020-02', '2020-03', '2020-04', '2020-05'] #feb -> may

import os
import sys
import pandas as pd

from geopy.geocoders import Nominatim
from datetime import datetime
#from pathlib import Path

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

dateTimeFormat = "%a %b %d %H:%M:%S %z %Y"

def insert(df, row):
    insert_loc = df.index.max()
    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

def main():
    global data_dirs
    global dateTimeFormat
    geolocator = Nominatim(user_agent="covid19_researcher")

    if len(sys.argv) != 2:
        print("Incorrect usage!")
        print("python analyze.py <parent directory of all data folders>")
        exit()
    fn = sys.argv[1]
    if not os.path.exists(fn):
        print("the path '" + fn + "' does not exist!")
        exit()
    
    data_dirs = list(map(str, [x for x in Path(fn).iterdir() if x.is_dir()]))

    data_df = pd.DataFrame(columns = ["time_created", "tweet_id", "user_id", "place_country", "place_full", "lat", "lon", "text"])
    for data_dir in data_dirs:
        for path in Path(data_dir).iterdir():
            if path.name.endswith('.csv'):
                data_df = data_df.append(process(path), ignore_index=True)

    #visualization goes here
    textDate = pd.DataFrame(columns = ["date_time", "state", "sentiment"])
    analyser = SentimentIntensityAnalyzer()
    for index, row in data_df.iterrows():
        state = "None"
        if row["lat"] == "None" or row["lon"] == "None":
            try:
                loc = geolocator.geocode(row["place_full"], addressdetails=True)
                state = loc.raw["address"]["state"]
            except:
                pass
        else:
            try:
                loc = geolocator.reverse(row["lat"] + ", " + row["lon"], addressdetails=True)
                state = loc.raw["address"]["state"]
            except:
                pass
        #might not be the most efficient code!
        insert(textDate, [datetime.strptime(row["time_created"], dateTimeFormat), state, analyser.polarity_scores(row["text"])["compound"]])
    
    stateFilter = ["oklahoma", "new york"]

    import matplotlib.pyplot as plt
    stateData = textDate.groupby(['state'])
    for index, row in stateData.iterrows():
        if row["state"].lower() not in stateFilter and len(stateFilter) != 0:
            print("Skipping " + row["state"])
            continue
        resampled = row.resample('W', on='date_time').mean()
        #TODO: write to CSV
        
        #plt.plot(resampled["date_time"], resampled["sentiment"], label=row["state"])
        #plt.plot(row["date_time"], row["sentiment"], label=row["state"])
    
    #plt.legend(loc="upper left")
    #plt.show()

def process(file):
    return pd.read_csv(file, header=None, names=["time_created", "tweet_id", "user_id", "place_country", "place_full", "lat", "lon", "text"])

if __name__ == "__main__":
    main()