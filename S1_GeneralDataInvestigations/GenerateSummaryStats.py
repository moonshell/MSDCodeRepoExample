#! /usr/bin/python3

# Import statements
import os
import sys
import pandas as pd
from datetime import datetime
DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, DIR)
from S0_HelperClassLibrary.ReadInArgs import readArgs
from S0_HelperClassLibrary.DatasetSummary import datasetSummary
from S0_HelperClassLibrary.CreateFileNames import *

"""GenerateSummaryStats.py is a script to find out more about the dataset quickly.

This script is the first script in General Data Investigations.  It is used to read in a json lines dataset and produce
standard information about the dataset for the first view of a new dataset, including:
    - File Info (Lists fields and field types)
    - File Shape (Number of rows and columns)
    - File Description (Provides count, mean, std, min, 25%, 50%, 75%, and max for numeric values)
    - File Header (Prints first 5 records)

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Generate Summary Statistics: Info, Shape, Description, Head
def generateSummaryStats(inputPath, dsName, dsNameExtension):
    # Detail inputFile, outputFile, and outputResults strings
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputResults = createOutputResultsName(dsName, "SummaryStats")

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)

    # Read in data with chunksize to reduce CPU needs
    if dsNameExtension == "jsonl":
        chunkedData = pd.read_json(inputFile, lines=True, chunksize=100000)
    elif dsNameExtension == "csv":
        chunkedData = pd.read_csv(inputFile, chunksize=100000)
    else:
        print("Data set needs to be csv or jsonl.")

    # Append chunked data into 1 data frame
    fullData = pd.concat(chunkedData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Produce Summary Stats on the dataset
    datasetSummary(fullData, inputFile, outputResults)


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateSummaryStats with the passed in arguments
generateSummaryStats(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)