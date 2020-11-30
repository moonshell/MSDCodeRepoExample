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
from S0_HelperClassLibrary.DatasetSummary import datasetShapeInfo
from S0_HelperClassLibrary.CreateFileNames import *

"""InvestigateAndTagDuplicates.py is a script that explores and identifies reversals and duplicate transactions.

This script expects a jsonl dataset sorted by customer id, account number, transaction amount, and transaction date-time.
There are multiple calculations to determine the difference from the previous transaction in amount, time, and other
critical fields.

The script identifies transactions that are considered duplicates.  Duplicates are defined as:
    - Multiple swipes of purchases within 1 minute
    - Reversals of purchase transactions within 1 month

The output results are saved in the Output/ directory.
"""
print(__doc__)
# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Generate Histograms
def investigateAndTagDuplicates(inputPath, dsName, dsNameExtension):
    # Concatenate arguments to obtain input and output file locations.
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputFile = createOutputFileName(inputPath, dsName, "jsonl", "DuplicatesIdentified", conversion=0)
    outputResults = createOutputResultsName(dsName, "DuplicatesIdentified")

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    # For testing use nrows=100000
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=10000, nrows=10000)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Convert transaction date time to a date type
    transactionData['gTransactionDateTime'] = pd.to_datetime(transactionData['transactionDateTime'])

    # Create empty lists
    indChangeInCustomerIdList = []
    indDuplicateTransactionList = []
    indMerchNameMatchList = []
    changeInTransactionAmountList = []
    changeInTransactionTimeList = []
    changeInTransactionTimeMonthsList = []
    changeInTransactionTimeDaysList = []
    changeInTransactionTimeMinutesList = []
    customerIndexList = []

    # Initialize customer references
    customerIndex = 1
    previousCustomerId = 0

    for index, row in transactionData.iterrows():
        # If customer id from this row doesn't match previous customer id, no changes exist in fields of interest.
        if row['customerId'] != previousCustomerId:
            customerIndex = customerIndex + 1
            indChangeInCustomerId = 'True'
            indMerchNameMatch = 'False'
            changeInTransactionAmount = None
            changeInTransactionTime = None
            changeInTransactionTimeMonths = None
            changeInTransactionTimeDays = None
            changeInTransactionTimeMinutes = None
        # If customer id from this row does match previous customer id, calculate differences in fields of interest.
        elif row['customerId'] == previousCustomerId:
            customerIndex = previousCustomerIndex
            indChangeInCustomerId = 'False'
            changeInTransactionAmount = row['transactionAmount'] - previousTransactionAmount
            if row['merchantName'] == previousMerchantName:
                indMerchNameMatch = 'True'
            else:
                indMerchNameMatch = 'False'
            changeInTransactionTime = row['gTransactionDateTime'] - previousTransactionDateTime
            changeInTransactionTimeMonths = changeInTransactionTime / np.timedelta64(1, "M")
            changeInTransactionTimeDays = changeInTransactionTime / np.timedelta64(1, "D")
            changeInTransactionTimeMinutes = changeInTransactionTime / np.timedelta64(1, "m")

        # Transaction processed, set previous values for processing of next record
        previousCustomerId = row['customerId']
        previousCustomerIndex = customerIndex
        previousTransactionAmount = row['transactionAmount']
        previousMerchantName = row['merchantName']
        previousTransactionDateTime = row['gTransactionDateTime']

        # Flag duplicate transactions, set default to false
        indDuplicateTransaction = 'False'

        # Reversals are duplicates of purchases and can occur within 1 month
        if row['transactionType'] == 'REVERSAL':
            if indChangeInCustomerId == 'False' and \
                changeInTransactionAmount == 0 and \
                indMerchNameMatch == 'True' and \
                changeInTransactionTimeMonths <= 1 and \
                changeInTransactionTimeMonths >= 0:
                indDuplicateTransaction = 'True'

        # Duplicate purchases are multiple swipes and occur within 1 minute
        elif row['transactionType'] == 'PURCHASE':
            if indChangeInCustomerId == 'False' and \
                changeInTransactionAmount == 0 and \
                indMerchNameMatch == 'True' and \
                changeInTransactionTimeMinutes <= 1 and \
                changeInTransactionTimeMinutes >= 0:
                indDuplicateTransaction = 'True'

        # Append this transactions information to the lists
        indChangeInCustomerIdList.append(indChangeInCustomerId)
        indDuplicateTransactionList.append(indDuplicateTransaction)
        indMerchNameMatchList.append(indMerchNameMatch)
        changeInTransactionAmountList.append(changeInTransactionAmount)
        changeInTransactionTimeList.append(changeInTransactionTime)
        changeInTransactionTimeMonthsList.append(changeInTransactionTimeMonths)
        changeInTransactionTimeDaysList.append(changeInTransactionTimeDays)
        changeInTransactionTimeMinutesList.append(changeInTransactionTimeMinutes)
        customerIndexList.append(customerIndex)

    # Add lists to the dataframe, g fields are generated
    transactionData['gIndChangeInCustomerId'] = indChangeInCustomerIdList
    transactionData['gIndDuplicateTransaction'] = indDuplicateTransactionList
    transactionData['gIndMerchNameMatch'] = indMerchNameMatchList
    transactionData['gChangeInTransactionAmount'] = changeInTransactionAmountList
    transactionData['gChangeInTransactionTime'] = changeInTransactionTimeList
    transactionData['gChangeInTransactionTimeMonths'] = changeInTransactionTimeMonthsList
    transactionData['gChangeInTransactionTimeDays'] = changeInTransactionTimeDaysList
    transactionData['gChangeInTransactionTimeMinutes'] = changeInTransactionTimeMinutesList

    # Write out dataset
    transactionData.to_json(outputFile, orient="records", lines=True)

    datasetShapeInfo(transactionData, inputFile, outputResults)

    with open(outputResults, 'a+') as output:
        # Print the number of identified duplicate records by transaction type
        print("---------Number of Duplicate Records By Transaction Type--------", file=output)
        print(pd.crosstab(transactionData['gIndDuplicateTransaction'], transactionData['transactionType']), file=output)

        # Calculate amounts associated with reversals and duplicate transactions
        print("---------Total Amount Associated with Duplicate Records--------", file=output)
        x1 = transactionData.loc[transactionData['gIndDuplicateTransaction'] == 'True']
        sumTotal = x1['transactionAmount'].sum()
        sumReversals = x1.loc[x1['transactionType'] == 'REVERSAL', 'transactionAmount'].sum()
        sumPurchases = x1.loc[x1['transactionType'] == 'PURCHASE', 'transactionAmount'].sum()

        print(sumTotal, file=output)
        print("---------Amount Associated with Reversal Duplicate Records--------", file=output)
        print(sumReversals, file=output)
        print("---------Amount Associated with Purchase Duplicate Records--------", file=output)
        print(sumPurchases, file=output)

        print("---------Total Amount Associated with all REVERSAL Records--------", file=output)
        print(transactionData.loc[transactionData['transactionType'] == 'REVERSAL', 'transactionAmount'].sum(), file=output)

    output.close()


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
investigateAndTagDuplicates(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
