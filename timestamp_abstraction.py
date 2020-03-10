import pandas as pd
import sys
import argparse


def retrieve_args():
    parser = argparse.ArgumentParser(description='Generalize timestamps in event log')
    parser.add_argument("inputFile",
                        help="original event log")
    parser.add_argument("outputFile",
                        help="Path for output")
    parser.add_argument("abstractionLevel",
                        help="How to abstract the timestamps")
    args = parser.parse_args()
    return args.inputFile, args.abstractionLevel, args.outputFile


def run_abstraction(eventLog,abstractionLevel):
    eventLog[colNameTimeStamp] = pd.to_datetime(eventLog[colNameTimeStamp], format=dateTimeFormat)
    eventLog[colNameTimeStamp] = eventLog[colNameTimeStamp].apply(lambda x: x.ceil(freq=abstractionLevel))
    return eventLog


# Define constants
colNameTimeStamp = "Complete Timestamp"
dateTimeFormat = "%Y/%m/%d %H:%M:%S"


# Main
inputFile, abstractionLevel, outputFile = retrieve_args()
eventLog = pd.read_csv(inputFile, delimiter=";")
eventLog = run_abstraction(eventLog, abstractionLevel)
eventLog.to_csv(outputFile)
