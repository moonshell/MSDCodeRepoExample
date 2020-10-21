#! /usr/bin/python3
"""ProduceCorrelationMatrixWithTag.py is a script that produces a correlation matrix and plots each variables with the tag.

This script expects a jsonl dataset.  The code produces a corelation matrix based on the method as defined in the
arguments.  In addition, there is a cross frequnecy of each field with the fraud tag, and two histogram of
each fields - 1 for non-frauds and 1 for frauds.

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
        InvestigateAndTagDuplicates.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

    Parameters
    __________
    inputPath : str
        The path of the input dataset.
    dsName : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt
    correlationMethod : str
        The method used the correlation matrix. Default = Pearson, Options kendall, spearman

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
    correiatonMethod = 'Pearson'

    # Three parameters are expected and h for help is also available
    try:
        opts, args = getopt.getopt(argv, "h", ["inputPath=", "dsName=", "dsNameExtension=", "correlationMethod"])
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
        elif opt in '--correlationMethod':
            correlationMethod = arg

    # Concatenate arguments to obtain input and output file locations.
    inputFile = str(inputPath + dsName + dsNameExtension)
    print("Input file is: " + inputFile)
    outputResults = 'Output/Output.Correlation.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return (inputFile, outputResults, correlationMethod)


# Generate Histograms
def produceCorrelationStats(inputFile, outputResults, correlationMethod):
    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Produce Frequencies on each field in the dataset
    with open(outputResults, 'w') as output:
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output results are found here: ' + outputResults)

        print("---------Correlation Matrix--------", file=output)
        if correlationMethod == 'pearson':
            print(transactionData.corr(method="pearson"), file=output)
        elif correlationMethod == 'kendall':
            print(transactionData.corr(method="kendall"), file=output)
        elif correlationMethod == 'spearman':
            print(transactionData.corr(method="spearman"), file=output)

        for column in transactionData:
            # Produce type, count, unique, and frequency information regardless of data type
            print("---------" + column + " Crossed with Fraud Indicator--------", file=output)
            print(pd.crosstab(transactionData[column], transactionData['isFraud']), file=output)

    output.close()

    for column in transactionData:
        print(column)
        if (column == 'isFraud' or column == 'gTransactionKey' or column == 'accountNumber' or column == 'customerId' or
                column == 'merchantName' or column == 'posConditionCode'):
            print("No histogram for:" + column)
        else:
             # Plot transaction type when transaction amount = 0 and positive
             if np.dtype(transactionData[column]) == 'bool':
                  transactionData[column] = transactionData[column].astype('str')

             x1 = transactionData.loc[transactionData['isFraud'].values, column]
             plt.hist(x1, color='b', label='Fraud')
             plt.title(column + ' for frauds')
             plt.xlabel("Value")
             plt.ylabel("Frequency")
             plt.legend()
             plt.savefig("Output/Histogram.ModelingPop." + column + ".frauds.png")
             plt.clf()

             # Plot transaction type when transaction amount = 0 and positive
             x2prep = np.invert(transactionData['isFraud'])
             x2 = transactionData.loc[x2prep.values, column]
             plt.hist(x2, color='r', label='NonFraud')
             plt.title(column + ' for nonfrauds')
             plt.xlabel("Value")
             plt.ylabel("Frequency")
             plt.legend()
             plt.savefig("Output/Histogram.ModelingPop." + column + ".nonfrauds.png")
             plt.clf()


# Create infile and outfile
(inputFile, outputResults, correlationMethod) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
produceCorrelationStats(inputFile, outputResults, correlationMethod)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
