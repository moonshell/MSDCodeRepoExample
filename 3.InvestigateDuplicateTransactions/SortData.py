#! /usr/bin/python3
""""SortData.py is a script that sorts data in ascending customer id, account id, transaction amount, transaction date-time.

This script expects the data to be a json lines dataset.  Both before and after the sort, there is a print of the first
five records.

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Import statements
import os
import sys
import getopt
import pandas as pd
from datetime import datetime

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Read in parameters
def readArgs(argv):
    """
    Run script with following command:
        SortData.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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
    outputFile : str
        The output dataset to be returned by the main function.
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
    outputFile = str(inputPath + dsName + '.sorted' + dsNameExtension)
    print("Output file is: " + outputFile)
    outputResults = 'Output/Output.Sort.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return (inputFile, outputFile, outputResults)


# Generate Histograms
def sortData(inputFile, outputFile, outputResults):
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
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output file is: ' + outputFile)
        print('Output results are found here: ' + outputResults)
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
