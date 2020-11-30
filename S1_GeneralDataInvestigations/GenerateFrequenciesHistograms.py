#! /usr/bin/python3
# Import statements
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, DIR)
from S0_HelperClassLibrary.ReadInArgs import readArgs
from S0_HelperClassLibrary.DatasetSummary import datasetShapeInfo
from S0_HelperClassLibrary.CreateFileNames import *

"""GenerateFrequenciesHistograms.py is a script that produces frequencies and histograms for every field.

This script expects a json lines dataset and produces the following information for each field:
    - Type of the field
    - Count of valid values
    - Unique number of values
    - Frequency
    - Histograms with a maximum of 20 unique values for strings, and by year for dates

There are some type conversions that are performed prior to producing the above information.
    - Booleans are converted to strings.
    - Object fields that have the word 'date' or 'Date' in the field name are converted to a date type.

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)


# Generate Frequencies
def generateFrequenciesHistograms(inputPath, dsName, dsNameExtension):
    # Detail inputFile, outputFile, and outputResults strings
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputResults = createOutputResultsName(dsName, "Frequencies")

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    chunkedData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # Append chunked data into 1 data frame
    fullData = pd.concat(chunkedData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Produce General Stats on Dataset
    datasetShapeInfo(fullData, inputFile, outputResults)

    # Produce Frequencies on each field in the dataset
    with open(outputResults, 'a+') as output:
        print('Output histograms are found here: Output/Histogram.*.png', file=output)
        for column in fullData:
            # Produce type, count, unique, and frequency information regardless of data type
            print("---------" + column + "--------", file=output)
            print("Type of " + column + " is: ", np.dtype(fullData[column]), file=output)
            print("Count of " + column + " is: " + str(fullData[column].count()), file=output)
            print("Number of unique values of " + column + " is: ", fullData[column].nunique(), file=output)
            print("Frequency of " + column + " is:", file=output)
            print(fullData[column].value_counts().sort_index(0), file=output)

            # For numeric, produce summary stats and histograms with 10 bins
            # Note: The descriptive stats are duplicates from the Generate Summary Stata, but aligned with other info
            # that will be contained in the appendix in the technical review.
            if np.dtype(fullData[column]) == 'int64' or np.dtype(fullData[column]) == 'float64':
                print(fullData[column].describe(), file=output)
                # if fullData.count(column) != 0:
                if fullData[column].count() != 0:
                    plt.hist(fullData[column], bins=10)
                    plt.title(column)
                    plt.xlabel("Value")
                    plt.ylabel("Frequency")
                    plt.savefig("Output/Histogram." + dsName + '_' + column + ".png")
                    plt.clf()
                else:
                    print("No histogram for " + column + " as data is all null.", file=output)
            # For Date fields, change type to Date, extract year and plot histogram.
            # Note: There is a dependency on the field to have 'Date' in it's name.
            elif np.dtype(fullData[column]) == 'object' and ("Date" in column or "date" in column):
                fullData[column] = pd.to_datetime(fullData[column])
                print("NEW Type of " + column + " is: ", np.dtype(fullData[column]), file=output)
                fullData[column + '_year'] = pd.DatetimeIndex(fullData[column]).year
                plt.hist(fullData[column + '_year'], bins=10)
                plt.title(column + '_year')
                plt.xlabel("Year")
                plt.xticks(rotation='vertical')
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + dsName + '_' + column + ".png")
                plt.clf()
            # For remaining objects - strings, produce summary stats and histograms.
            elif np.dtype(fullData[column]) == 'object' and fullData[column].nunique() < 20:
                plt.hist(fullData[column])
                plt.title(column)
                plt.xlabel("Value")
                plt.xticks(rotation='vertical')
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + dsName + '_' + column + ".png")
                plt.clf()
            # For boolean fields, change type to string, and plot histogram.
            elif np.dtype(fullData[column]) == 'bool':
                fullData[column + "_str"] = fullData[column].astype('str')
                plt.hist(fullData[column + "_str"])
                plt.title(column + '_str')
                plt.xlabel("Value")
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + dsName + '_' + column + ".png")
                plt.clf()
    output.close()


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
generateFrequenciesHistograms(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
