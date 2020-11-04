from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

import pathlib
import pandas as pd

arr = []

for file in pathlib.Path("out").glob("*.csv"):
    arr.append(file.stem)

state_completer = WordCompleter(arr)
text = prompt('Enter States: ', completer=state_completer)

STATES = text.split(" ") #["California", "NewYork"]

pTopics = ["nrc_fear", "nrc_anger", "nrc_anticipation", "nrc_trust", "nrc_surprise", "nrc_positive", "nrc_negative", "nrc_sadness", "nrc_disgust", "nrc_joy", "sentiment"]

state_completer = WordCompleter(pTopics)
text = prompt('Enter Topics: ', completer=state_completer)
TOPICS = text.split(" ") #["nrc_anger"]

dataFrames = dict()
for state in STATES:
    statePath = pathlib.Path("out") / pathlib.Path(state + ".csv")
    if not statePath.exists():
        print("Couldn't find state '" + state + "'")
        exit()
    dataFrames[state] = pd.read_csv(statePath, parse_dates=["date_time"]).resample('10D', on='date_time').mean().reset_index()

import matplotlib.pyplot as plt

for name, df in dataFrames.items():
    for topic in TOPICS:
        print(df[topic])
        plt.plot(df["date_time"], df[topic], label=topic + "(" + name + ")")

plt.legend()
plt.show()