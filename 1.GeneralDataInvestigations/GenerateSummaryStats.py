#! /usr/bin/python3
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
        GenerateSummaryStatistics.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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

    inputFile = str(inputPath + dsName + dsNameExtension)
    print("Input file is: " + inputFile)
    outputResults = 'Output/Output.SummaryStats.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return (inputFile, outputResults)


# Generate Summary Statistics: Info, Shape, Description, Head
def generateSummaryStats(inputFile, outputResults):
    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Produce Summary Stats on the dataset
    with open(outputResults, 'w') as output:
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output results are found here: ' + outputResults)
        print("---------File Info--------", file=output)
        print(transactionData.info(buf=output))
        print("File Info Complete")
        print("---------File Shape--------", file=output)
        print(transactionData.shape, file=output)
        print("File Shape Complete")
        print("---------File Description of Fields--------", file=output)
        print(transactionData.describe(), file=output)
        print("File Description Complete")
        print("---------File Header--------", file=output)
        print(transactionData.head(), file=output)
        print("File Header Complete")
    output.close()


# Create infile and outfile
(inputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateSummaryStats with the passed in arguments
generateSummaryStats(inputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)