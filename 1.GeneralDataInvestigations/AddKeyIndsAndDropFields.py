#! /usr/bin/python3
"""AddKeyIndsAndDropFields.py is a script to add fields needed for use in further investigations.

This script expects a jsonl dataset.  It is used to generate indicators and create a primary key for each transaction
in the file.  The indicators that are calculated are:
    - gIndCustIdEqAcctNum to understand the uniqueness across the two fields of customerId and accountNumber
    - gIndCardCvvEqEnteredCVV to understand the difference between the actual and entered CVV
    - gIndDtAddrChangeEqAcctOpenDt to investigate the presences of DtAddrChange being 100% present

With the new dataframe, the following is produced to track the changes in the dataset:
    - File Info (Lists fields and field types)
    - File Shape (Number of rows and columns)

With the new variables, the following is produced:
    - Number of unique values of the new and old keys
    - Frequencies of the new indicators

The output results are saved in the Output/ directory.
"""
print(__doc__)
# Import statements
import os
import sys
import getopt
import pandas as pd
import numpy as np
from datetime import datetime

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)


# Read in parameters
def readArgs(argv):
    """
    Run script with following command:
        AddKeyIndsAndDropFields.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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
         The output file that contains the output dataset.
    outputResults : str
         The output file that contains the results of functions.

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
    outputFile = str(inputPath + dsName + '.withKey' + dsNameExtension)
    print("Output file is: " + outputFile)
    outputResults = 'Output/Output.AddKeyAndDropFields.' + dsName + '.txt'
    print("Output results are found here: " + outputResults)

    return inputFile, outputFile, outputResults


# Add key and drop fields
def addKeyAndDropFields(inputFile, outputFile, outputResults):
    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)

    # Read in data with chunksize to reduce CPU needs
    # For testing set chunksize and nrows to desired row count
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=10, nrows=10)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Change date fields to have a type of date for easier processing later
    for column in transactionData:
        if np.dtype(transactionData[column]) == 'object' and ("Date" in column or "date" in column):
            transactionData[column + "DtType"] = pd.to_datetime(transactionData[column])

    # Convert customerId and accountNumber and transactionDateTime to strings
    transactionData['custString'] = transactionData['customerId'].astype(str)
    transactionData['acctString'] = transactionData['accountNumber'].astype(str)
    transactionData['dttmString'] = transactionData['transactionDateTime'].astype(str)

    # Append three string fields created above to get a unique transaction id
    transactionData['gTransactionKey'] = transactionData['custString'].str.cat(transactionData['acctString'])
    transactionData['gTransactionKey'] = transactionData['gTransactionKey'].str.cat(transactionData['dttmString'])

    # Add indicator when transaction information matches
    transactionData['gIndCustIdEqAcctNum'] = np.where(transactionData['customerId'] == transactionData['accountNumber'], "Matched", "Not Matched")
    transactionData['gIndCardCvvEqEnteredCVV'] = np.where(transactionData['cardCVV'] == transactionData['enteredCVV'], "Matched", "Not Matched")
    transactionData['gIndDtAddrChangeEqAcctOpenDt'] = np.where(transactionData['dateOfLastAddressChange'] == transactionData['accountOpenDate'], "Matched", "Not Matched")

    # Drop fields with 100% missing values and intermediate variables
    # These fields were identified using the GenerateFrequenciesHistograms.py script
    transactionData = transactionData.drop(['echoBuffer', 'merchantCity', 'merchantState', 'merchantZip',
                                        'posOnPremises', 'recurringAuthInd', 'custString', 'acctString', 'dttmString'], axis=1)

    # Write out dataset
    transactionData.to_json(outputFile, orient="records", lines=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Produce Frequencies on each field in the dataset
    with open(outputResults, 'w') as output:
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output file is: ' + outputFile)
        print('Output results is: ' + outputResults)

        # Print file info to confirm date types on date fields
        print("---------File Info--------", file=output)
        print(transactionData.info(buf=output))

        # Print file shape to track size of data
        print("---------File Shape--------", file=output)
        print(transactionData.shape, file=output)

        # Print number of unique
        print("---------Unique Trans Keys--------", file=output)
        print(transactionData['gTransactionKey'].nunique(), file=output)
        print("---------Unique Customer Id--------", file=output)
        print(transactionData['customerId'].nunique(), file=output)

        # Print frequency of new indicator variables
        print("---------Frequency of gIndCustIdEqAcctNum--------", file=output)
        print(transactionData['gIndCustIdEqAcctNum'].value_counts().sort_index(0), file=output)

        print("---------Frequency of gIndCardCvvEqEnteredCVV--------", file=output)
        print(transactionData['gIndCardCvvEqEnteredCVV'].value_counts().sort_index(0), file=output)

        print("---------Frequency of gIndCardCvvEqEnteredCVV--------", file=output)
        print(transactionData['gIndDtAddrChangeEqAcctOpenDt'].value_counts().sort_index(0), file=output)
    output.close()


# Create infile and outfile
(inputFile, outputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
addKeyAndDropFields(inputFile, outputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
