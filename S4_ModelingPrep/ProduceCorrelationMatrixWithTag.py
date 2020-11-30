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
from S0_HelperClassLibrary.ReadInArgs import readArgsWithCorrelationAndTag
from S0_HelperClassLibrary.DatasetSummary import datasetShapeInfo
from S0_HelperClassLibrary.CreateFileNames import *

"""ProduceCorrelationMatrixWithTag.py is a script that produces a correlation matrix and plots each variables with the tag.

This script expects a jsonl dataset.  The code produces a correlation matrix based on the method as defined in the
arguments.  In addition, there is a cross frequency of each field with the fraud tag, and two histogram of
each fields - 1 for non-frauds and 1 for frauds.

The output results are saved in the Output/ directory.
"""
print(__doc__)
# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)


# Generate Histograms
def produceCorrelationStats(inputPath, dsName, dsNameExtension, correlationMethod, targetField):
    # Concatenate arguments to obtain input and output file locations.
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputResults = createOutputResultsName(dsName, "Correlation")

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
        print('Input file is: ' + inputFile, file=output)
        print('Output results are found here: ' + outputResults, file=output)

        print("---------Correlation Matrix--------", file=output)
        if correlationMethod == 'pearson':
            print(transactionData.corr(method="pearson"), file=output)
        elif correlationMethod == 'kendall':
            print(transactionData.corr(method="kendall"), file=output)
        elif correlationMethod == 'spearman':
            print(transactionData.corr(method="spearman"), file=output)

        for column in transactionData:
            # Produce type, count, unique, and frequency information regardless of data type
            print("---------" + column + " Crossed with Target--------", file=output)
            print(pd.crosstab(transactionData[column], transactionData[targetField]), file=output)

    output.close()

    for column in transactionData:
        print(column)
        if (column == targetField or column == 'gTransactionKey' or column == 'accountNumber' or column == 'customerId' or
                column == 'merchantName' or column == 'posConditionCode'):
            print("No histogram for:" + column)
        else:
             # Plot transaction type when transaction amount = 0 and positive
             if np.dtype(transactionData[column]) == 'bool':
                  transactionData[column] = transactionData[column].astype('str')

             x1 = transactionData.loc[transactionData[targetField].values, column]
             plt.hist(x1, color='b', label='Fraud')
             plt.title(column + ' for frauds')
             plt.xlabel("Value")
             plt.ylabel("Frequency")
             plt.legend()
             plt.savefig("Output/Histogram.ModelingPop." + column + ".frauds.png")
             plt.clf()

             # Plot transaction type when transaction amount = 0 and positive
             x2prep = np.invert(transactionData[targetField])
             x2 = transactionData.loc[x2prep.values, column]
             plt.hist(x2, color='r', label='NonFraud')
             plt.title(column + ' for nonfrauds')
             plt.xlabel("Value")
             plt.ylabel("Frequency")
             plt.legend()
             plt.savefig("Output/Histogram.ModelingPop." + column + ".nonfrauds.png")
             plt.clf()


# Create infile and outfile
(inputPath, dsName, dsNameExtension, correlationMethod, targetField) = readArgsWithCorrelationAndTag(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
produceCorrelationStats(inputPath, dsName, dsNameExtension, correlationMethod, targetField)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
