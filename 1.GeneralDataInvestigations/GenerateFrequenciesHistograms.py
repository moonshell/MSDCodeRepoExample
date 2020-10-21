#! /usr/bin/python3
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

# Import statements
import os
import sys
import getopt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)


# Read in parameters
def readArgs(argv):
    """
    Run script with following command:
        GenerateFrequenciesHistograms.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>'

    Parameters
    __________
    inputPath : str
        The path of the input dataset.
    dsName : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt

    Return
    _______
    inputFile : str
        The input dataset to be utilized by the main function.
    outputResults : str
         The output file that contains the results of functions.  This is not the output dataset.

    """
    inputPath = ''
    dsName = ''
    dsNameExtension = ''

    # Three parameters are expected and h for help is also available
    try:
        opts, args = getopt.getopt(argv, "h", ["inputPath=", "dsName=", "dsNameExtension="])
        print("The input arguments are: ", opts, args)
    except getopt.GetoptError as error:
        print(readArgs.__doc__)
        print("Need passed parameters.  Code did not run.")
        sys.exit()

    # Set values for each parameter with the contents of the argument
    for opt, arg in opts:
        if opt in '-h':
            print(readArgs.__doc__)
            sys.exit()
            print("Need passed parameters.  Code did not run.")
        elif opt in '--inputPath':
            inputPath = arg
        elif opt in '--dsName':
            dsName = arg
        elif opt in '--dsNameExtension':
            dsNameExtension = arg

    # Concatenate arguments to obtain input and output file locations.
    inputFile = str(inputPath + dsName + dsNameExtension)
    print("Input file is: " + inputFile)
    outputResults = 'Output/Output.Frequencies.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return (inputFile, outputResults)


# Generate Frequencies
def generateFrequenciesHistograms(inputFile, outputResults):
    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Produce Frequencies on each field in the dataset
    with open(outputResults, 'w') as output:
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output results are found here: ' + outputResults)
        print('Output histograms are found here: Output/Histogram.*.png')
        for column in transactionData:
            # Produce type, count, unique, and frequency information regardless of data type
            print("---------" + column + "--------", file=output)
            print("Type of " + column + " is: ", np.dtype(transactionData[column]), file=output)
            print("Count of " + column + " is: " + str(transactionData[column].count()), file=output)
            print("Number of unique values of " + column + " is: ", transactionData[column].nunique(), file=output)
            print("Frequency of " + column + " is:", file=output)
            print(transactionData[column].value_counts().sort_index(0), file=output)

            # For numeric, produce summary stats and histograms with 10 bins
            # Note: The descriptive stats are duplicates from the Generate Summary Stata, but aligned with other info
            # that will be contained in the appendix in the technical review.
            if np.dtype(transactionData[column]) == 'int64' or np.dtype(transactionData[column]) == 'float64':
                print(transactionData[column].describe(), file=output)
                plt.hist(transactionData[column], bins=10)
                plt.title(column)
                plt.xlabel("Value")
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + column + ".png")
                plt.clf()
            # For Date fields, change type to Date, extract year and plot histogram.
            # Note: There is a dependency on the field to have 'Date' in it's name.
            elif np.dtype(transactionData[column]) == 'object' and ("Date" in column or "date" in column):
                transactionData[column] = pd.to_datetime(transactionData[column])
                print("NEW Type of " + column + " is: ", np.dtype(transactionData[column]), file=output)
                transactionData[column + '_year'] = pd.DatetimeIndex(transactionData[column]).year
                plt.hist(transactionData[column + '_year'], bins=10)
                plt.title(column + '_year')
                plt.xlabel("Year")
                plt.xticks(rotation='vertical')
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + column + ".png")
                plt.clf()
            # For remaining objects - strings, produce summary stats and histograms.
            elif np.dtype(transactionData[column]) == 'object' and transactionData[column].nunique() < 20:
                plt.hist(transactionData[column])
                plt.title(column)
                plt.xlabel("Value")
                plt.xticks(rotation='vertical')
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + column + ".png")
                plt.clf()
            # For boolean fields, change type to string, and plot histogram.
            elif np.dtype(transactionData[column]) == 'bool':
                transactionData[column + "_str"] = transactionData[column].astype('str')
                plt.hist(transactionData[column + "_str"])
                plt.title(column + '_str')
                plt.xlabel("Value")
                plt.ylabel("Frequency")
                plt.savefig("Output/Histogram." + column + ".png")
                plt.clf()
    output.close()


# Create infile and outfile
(inputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
generateFrequenciesHistograms(inputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
