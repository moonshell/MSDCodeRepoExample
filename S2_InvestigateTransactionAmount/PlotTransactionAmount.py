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
from S0_HelperClassLibrary.CreateFileNames import *

""" PlotTransactionAmount.py performs a simple histogram and explores when behaviors when transaction amount = 0.

This script expects a json lines datasets.  It produces histograms of the raw transaction amount field in bins of 10 and
40.  For further analysis of transaction amount, an indicator variable gIndTransAmtEq0 is created to separate
transactions with 0 amounts from those with positive amounts.  With this indicator, the following exhibits are produced:
    - Histogram of Transaction Type by Indicator
    - Cross tabs of each field with the Indicator
    - Frequency of merchants with transaction amounts = 0

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Generate Histograms
def evaluateTransactionAmount(inputPath, dsName, dsExtension):
    # Detail inputFile, outputFile, and outputResults strings
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputResults = createOutputResultsName(dsName, "Histograms")

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Plot transaction amount with 10 bins
    plt.hist(transactionData['transactionAmount'], bins=10)
    plt.title('Transaction Amount')
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.savefig("Output/Histogram.transactionAmount.10bins.png")
    plt.clf()

    # Plot transaction amount with 40 bins
    plt.hist(transactionData['transactionAmount'], bins=40)
    plt.title('Transaction Amount')
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.savefig("Output/Histogram.transactionAmount.40bins.png")
    plt.clf()

    # Add indicator when transaction information matches
    transactionData['gIndTransAmtEq0'] = np.where(transactionData['transactionAmount'] == 0, "Zero", "Positive")

    # Plot transaction type when transaction amount = 0 and positive
    x1 = transactionData.loc[transactionData.gIndTransAmtEq0 == 'Zero', 'transactionType']
    x2 = transactionData.loc[transactionData.gIndTransAmtEq0 == 'Positive', 'transactionType']
    plt.hist(x1, color='b', label='TranAmt=Zero')
    plt.hist(x2, color='r', label='TranAmt=Positive')
    plt.title('Transaction Type by Transaction Amount Indicator')
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.legend()
    plt.savefig("Output/Histogram.transactionAmount.transactionType.png")
    plt.clf()


    # Produce cross-tabs of each field with transaction amount grouped (0, Positive)
    with open(outputResults, 'w') as output:
        print('Input file is: ' + inputFile, file=output)
        print('Output results are found here: ' + outputResults, file=output)
        print('Output histograms are found here: Output/Histogram.*.png', file=output)
        for column in transactionData:
            # Produce frequency
            print("---------" + column + "--------", file=output)
            print(pd.crosstab(transactionData[column], transactionData['gIndTransAmtEq0']), file=output)

        # Select merchants that have transaction values = 0
        x3 = transactionData.loc[transactionData.gIndTransAmtEq0 == 'Zero', 'merchantName']
        print("---------Merchant When Transaction Amount = 0 (Sorted by Name)--------", file=output)
        print(x3.value_counts().sort_index(0), file=output)
        print("---------Merchant When Transaction Amount = 0 (Sorted by Count)--------", file=output)
        print(x3.value_counts(), file=output)
    output.close()


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
evaluateTransactionAmount(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
