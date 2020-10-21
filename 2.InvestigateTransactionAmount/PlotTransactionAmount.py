#! /usr/bin/python3
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
        PlotTransactionAmount.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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
    outputResults = 'Output/Output.transactionAmount.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return (inputFile, outputResults)


# Generate Histograms
def evaluateTransactionAmount(inputFile, outputResults):
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
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output results are found here: ' + outputResults)
        print('Output histograms are found here: Output/Histogram.*.png')
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
(inputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
evaluateTransactionAmount(inputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
