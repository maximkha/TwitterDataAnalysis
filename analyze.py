#!/usr/bin/env python3

from tqdm import tqdm
from pathlib import Path
import preprocessor as p 
import reverse_geocoder as rg

import sys
import os

data_dirs = []

import pandas as pd
import abrevs
from datetime import datetime

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# https://pypi.org/project/NRCLex/
from nrclex import NRCLex

dateTimeFormat = "%a %b %d %H:%M:%S %z %Y"

def insert(df, row):
    insert_loc = df.index.max()
    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

def multiCategorySentiment(text):
    text_object = NRCLex(text)
    countDict = dict({
        'fear': 0, 
        'sadness': 0, 
        'negative': 0, 
        'disgust': 0, 
        'anticip':0, 
        'joy': 0,
        'trust': 0,
        'positive': 0,
        'surprise': 0,
        'anger': 0
        })
    countDict.update(text_object.raw_emotion_scores)
    total = sum(countDict.values())
    if total == 0:
        return countDict
    norm = {k: v/total for k, v in countDict.items()}
    return norm

def main():
    global data_dirs
    global dateTimeFormat

    stateName = abrevs.load("abrevs.txt")

    if len(sys.argv) != 2:
        print("Incorrect usage!")
        print("python analyze.py <parent directory of all data folders>")
        exit()
    fn = sys.argv[1]
    if not os.path.exists(fn):
        print("the path '" + fn + "' does not exist!")
        exit()
    #fn = r"E:\Akbas\data\coviddata"

    data_dirs = list(map(str, [x for x in Path(fn).iterdir() if x.is_dir()]))

    data_df = pd.DataFrame(columns = ["time_created", "tweet_id", "user_id", "place_country", "place_full", "lat", "lon", "text"])
    for data_dir in data_dirs:
        for path in Path(data_dir).iterdir():
            if path.name.endswith('.csv'):
                data_df = data_df.append(process(path), ignore_index=True)

    #visualization goes here
    textDate = pd.DataFrame(columns = [
        "date_time", 
        "state", 
        "sentiment", 
        "nrc_fear", 
        "nrc_anger", 
        "nrc_anticipation", 
        "nrc_trust", 
        "nrc_surprise", 
        "nrc_positive", 
        "nrc_negative", 
        "nrc_sadness", 
        "nrc_disgust", 
        "nrc_joy"
    ])
    
    analyser = SentimentIntensityAnalyzer()
    for index, row in tqdm(list(data_df.iterrows())):
        #only US
        if row["place_country"] != "US":
            continue

        state = "None"
        if row["lat"] == "None" or row["lon"] == "None":
            state = abrevs.parse_place(row["place_full"], stateName)[1]
        else:
            try:
                coordinates = (float(row["lat"]), float(row["lon"])),
                state = rg.search(coordinates, verbose=False)[0]["admin1"]
            except:
                pass

        #might not be the most efficient code!
        cleanText = p.clean(row["text"])
        
        posNegSentiment = analyser.polarity_scores(cleanText)["compound"]
        catSent = multiCategorySentiment(cleanText)
        
        insert(textDate, [
            datetime.strptime(row["time_created"], dateTimeFormat), 
            state, 
            posNegSentiment,
            catSent["fear"],
            catSent["anger"],
            catSent["anticip"],
            catSent["trust"],
            catSent["surprise"],
            catSent["positive"],
            catSent["negative"],
            catSent["sadness"],
            catSent["disgust"],
            catSent["joy"]
        ])
    
    stateFilter = []

    stateData = textDate.groupby(['state'])
    for state, row in stateData:
        print("state " + state)
        if state not in stateFilter and len(stateFilter) != 0:
            print("Skipping " + state)
            continue

        cleanState = ''.join(e for e in state if e.isalnum())

        row.to_csv(Path("out") / Path(cleanState + ".csv"))
    

def process(file):
    return pd.read_csv(file, header=None, names=["time_created", "tweet_id", "user_id", "place_country", "place_full", "lat", "lon", "text"])

if __name__ == "__main__":
    main()
