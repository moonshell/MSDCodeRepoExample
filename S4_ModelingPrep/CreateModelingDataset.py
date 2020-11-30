#! /usr/bin/python3
# Import statements
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, DIR)
from S0_HelperClassLibrary.ReadInArgs import readArgs
from S0_HelperClassLibrary.CreateFileNames import *

"""CreateModelingDataset.py is a script that selects final observations for the modeling dataset.

This script expects a jsonl dataset.  This script creates two datasets.
    - A dataset with all retained transactions with fraud indicator
    - A dataset containing one randomly selected observation for each customer id with fraud indicator.

The output results are saved in the Output/ directory.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Add key and drop fields
def addFirstFraudDataAndSelectObs(inputPath, dsName, dsNameExtension):
    # Concatenate arguments to obtain input and output file locations.
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputFile1 = createOutputFileName(inputPath, dsName, "jsonl", "withFraudInd", conversion=0)
    outputFile2 = createOutputFileName(inputPath, dsName, "jsonl", "modelingPopulation", conversion=0)
    outputResults = createOutputResultsName(dsName, "FirstFraudDate")


    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)

    # Read in data with chunksize to reduce CPU needs
    # For testing set chunksize and nrows to desired row count
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100, nrows=100)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    exclusionsList = []
    # Identify transactions to be excluded from modeling
    for index, row in transactionData.iterrows():
        exclusions = '0 - Not excluded'
        if row['acqCountry'] != 'US':
            exclusions = '1 - Not US Issuer'
        elif row['gIndDuplicateTransaction'] == 'True':
            exclusions = '2 - Duplicate Transaction'
        elif row['transactionType'] == 'REVERSAL':
            exclusions = '3 - Reversal'

        exclusionsList.append(exclusions)

    transactionData['gExclusions'] = exclusionsList

    # Removed excluded records from further processing
    exclusionsRemoved = transactionData[transactionData['gExclusions'] == '0 - Not excluded']

    # Sort data with descending customerId, isFraud, transactionDateTime
    dataSorted = exclusionsRemoved.sort_values(["customerId", "isFraud", "transactionDateTime"], ascending=False)

    # Identify first fraud for customer and tag which customers have a fraud
    # Initialize lists and fields
    dateFirstFraudList = []
    indFraudCustomerList = []
    indFirstFraudList = []
    previousCustomerId = 0
    firstFraud = 0

    for index, row in dataSorted.iterrows():
        # if customer id is new, then set previous to 0
        if row['customerId'] != previousCustomerId:
            previousCustomerId = row['customerId']
            indFraudCustomer = 0
            dateFirstFraud = 0
            previousIndFraudCustomer = 0
            previousDateFirstFraud = 0

            # if transaction is fraud, the set customer and first fraud fields
            if row['isFraud']:
                indFraudCustomer = 1
                previousIndFraudCustomer = 1

                dateFirstFraud = row['transactionDateTime']
                previousDateFirstFraud = dateFirstFraud

                firstFraud = 1
        else:
            indFraudCustomer = previousIndFraudCustomer
            dateFirstFraud = previousDateFirstFraud
            firstFraud = 0

            if row['isFraud'] and indFraudCustomer == 0:
                print("Check sort")

        # Append data to lists
        dateFirstFraudList.append(dateFirstFraud)
        indFraudCustomerList.append(indFraudCustomer)
        indFirstFraudList.append(firstFraud)

    # Add lists to the dataframe
    dataSorted['gDateFirstFraud'] = dateFirstFraudList
    dataSorted['gIndFraudCustomer'] = indFraudCustomerList
    dataSorted['gIndFirstFraud'] = indFirstFraudList
    dataSorted['gRandomNumber'] = np.random.randint(0, 10000, len(dataSorted.index))

    # Sort data with descending customerId, isFraud, transactionDateTime
    dataSortedByRandomNumber = dataSorted.sort_values(["customerId", "gRandomNumber"], ascending=False)

    # Randomly select transaction to be used for each customer if non-fraud
    # Select first fraud transaction for customers with fraud
    selectedObservationList = []
    previousCustomerId = 0
    for index, row in dataSortedByRandomNumber.iterrows():
        # If new customer and not a fraud customer, take the highest random value
        if row['customerId'] != previousCustomerId and row['gIndFraudCustomer'] == 0:
            previousCustomerId = row['customerId']
            selectedObservation = 1
        # If previous customer and not a fraud customer, take the highest random value
        elif row['gIndFraudCustomer'] == 1 and row['gIndFirstFraud'] == 1:
            selectedObservation = 1
        else:
            selectedObservation = 0

        selectedObservationList.append(selectedObservation)

    # Retain selected observations
    dataSortedByRandomNumber['gSelectedObservation'] = selectedObservationList
    dataFiltered = dataSortedByRandomNumber[dataSortedByRandomNumber['gSelectedObservation'] == 1]

    # Write out dataset with non-excluded transactions
    dataSorted.to_json(outputFile1, orient="records", lines=True)

    # Write out dataset with selected transactions
    dataFiltered.to_json(outputFile2, orient="records", lines=True)


    with open(outputResults, 'w') as output:
        print("--------Data Sorted---------", file=output)
        print(dataFiltered.info(buf=output))

        print("--------Number of Unique customers---------", file=output)
        print(dataSorted['customerId'].nunique(), file=output)

        print("--------Frequency of Exclusions---------", file=output)
        print(dataSorted['gExclusions'].value_counts().sort_index(0), file=output)

        print("--------Frequency of Customers with Fraud - All Transactions---------", file=output)
        print(dataSorted['gIndFraudCustomer'].value_counts().sort_index(0), file=output)

        print("--------Data Sorted with Random Number---------", file=output)
        print(dataFiltered.info(buf=output))

        print("--------Number of Unique customers---------", file=output)
        print(dataFiltered['customerId'].nunique(), file=output)

        print("--------Frequency of Customers with Fraud - Selected Transactions---------", file=output)
        print(dataFiltered['gIndFraudCustomer'].value_counts().sort_index(0), file=output)
    output.close()


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
addFirstFraudDataAndSelectObs(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)