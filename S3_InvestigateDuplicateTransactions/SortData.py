#! /usr/bin/python3
# Import statements
import os
import sys
import getopt
import pandas as pd
from datetime import datetime
DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, DIR)
from S0_HelperClassLibrary.ReadInArgs import readArgs
from S0_HelperClassLibrary.DatasetSummary import datasetShapeInfo
from S0_HelperClassLibrary.CreateFileNames import *

""""SortData.py is a script that sorts data in ascending customer id, account id, transaction amount, transaction date-time.

This script expects the data to be a json lines dataset.  Both before and after the sort, there is a print of the first
five records.

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Generate Histograms
def sortData(inputPath, dsName, dsNameExtension):
    # Concatenate arguments to obtain input and output file locations.
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputFile = createOutputFileName(inputPath, dsName, dsNameExtension, "Sorted", conversion=0)
    outputResults = createOutputResultsName(dsName, "SortData")

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    #chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=1000, nrows=100000)
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=10000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    with open(outputResults, 'w') as output:
        print('Input file is: ' + inputFile, file=output)
        print('Output file is: ' + outputFile, file=output)
        print('Output results are found here: ' + outputResults, file=output)
        print("---------File Header Before Sort--------", file=output)
        print(transactionData.head(), file=output)

        # Sort data by (customer id, account number, transaction amount, transaction date time)
        dataSorted = transactionData.sort_values(["customerId", "accountNumber", "transactionAmount", "transactionDateTime"])

        # Write out dataset
        dataSorted.to_json(outputFile, orient="records", lines=True)

        print("---------File Header After Sort--------", file=output)
        print(dataSorted.head(), file=output)
    output.close()


# Create infile and outfile
(inputFile, outputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
sortData(inputFile, outputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
