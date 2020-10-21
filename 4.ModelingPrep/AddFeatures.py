#! /usr/bin/python3
""""AddFeatures.py is a script that adds simple features to the dataset for use in modeling.

This script adds simple features including time since address change and a risk table of merchant category code.  This
script also adds a customer id hash and a partition group.

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
from pandas.util import hash_pandas_object

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)


# Read in parameters
def readArgs(argv):
    """
    Run script with following command:
        AddFeatures.py -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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
    outputFile = str(inputPath + dsName + '.withFeatures.txt')
    print("Output file is: " + outputFile)
    outputResults = 'Output/Output.AddFeatures.txt'
    print("Output results are found here: " + outputResults)

    return inputFile, outputFile, outputResults


# Add key and drop fields
def addFeatures(inputFile, outputFile, outputResults):
    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)

    # Read in data with chunksize to reduce CPU needs
    # For testing set chunksize and nrows to desired row count
    chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=100000)
    # chunkedTransactionData = pd.read_json(inputFile, lines=True, chunksize=10, nrows=10)
    # Append chunked data into 1 data frame
    transactionData = pd.concat(chunkedTransactionData, ignore_index=True)

    # Create Output directory if it doesn't exist
    if not os.path.exists('Output'):
        os.makedirs('Output')

    # Add hash for customer id
    transactionData['gCustomerIdHash'] = hash_pandas_object(transactionData['customerId'], index=True, categorize=True)

    # Add sample set for validation by taking last digit of customer id hash
    transactionData['gPartition'] = transactionData['gCustomerIdHash'] % 10

    # Add features that don't require previous transaction knowledge
    # Format dates
    transactionData['gTransactionDateTime'] = pd.to_datetime(transactionData['transactionDateTime'])
    transactionData['gDateOfLastAddressChange'] = pd.to_datetime(transactionData['dateOfLastAddressChange'])
    transactionData['gAccountOpenDate'] = pd.to_datetime(transactionData['accountOpenDate'])

    # Calculate number of days since address change.  Default value = 365 which is higher than the maximum calculated value.
    transactionData['gNumDaysSinceLastAddressChange'] = np.where(transactionData['dateOfLastAddressChange'] == transactionData['accountOpenDate'],
                                                                 365, (transactionData['gTransactionDateTime'] - transactionData['gDateOfLastAddressChange']) / np.timedelta64(1, "D"))

    # Calculate if move happened in last 30 days
    transactionData['gIndLastAddressChangeWithin30Days'] = np.where(transactionData['gNumDaysSinceLastAddressChange'] <= 30, 1, 0)

    # Calculate Months Open
    transactionData['gNumMonthsOpen'] = (transactionData['gTransactionDateTime'] - transactionData['gAccountOpenDate']) / np.timedelta64(1, "M")

    # Add risk table for merchant category code.  Risk table value is a smoothed WOE.
    transactionData.loc[transactionData['merchantCategoryCode'] == 'airline', 'gMerchantCategoryCodeRiskTable'] = -0.807063458
    transactionData.loc[transactionData['merchantCategoryCode'] == 'auto', 'gMerchantCategoryCodeRiskTable'] = 0.224553926
    transactionData.loc[transactionData['merchantCategoryCode'] == 'cable / phone', 'gMerchantCategoryCodeRiskTable'] = 3.098851402
    transactionData.loc[transactionData['merchantCategoryCode'] == 'entertainment', 'gMerchantCategoryCodeRiskTable'] = 0.277485754
    transactionData.loc[transactionData['merchantCategoryCode'] == 'fastfood', 'gMerchantCategoryCodeRiskTable'] = 0.505350348
    transactionData.loc[transactionData['merchantCategoryCode'] == 'food', 'gMerchantCategoryCodeRiskTable'] = 0.163152711
    transactionData.loc[transactionData['merchantCategoryCode'] == 'food_delivery', 'gMerchantCategoryCodeRiskTable'] = 4.567079146
    transactionData.loc[transactionData['merchantCategoryCode'] == 'fuel', 'gMerchantCategoryCodeRiskTable'] = 5.949616458
    transactionData.loc[transactionData['merchantCategoryCode'] == 'furniture', 'gMerchantCategoryCodeRiskTable'] = 0.122767859
    transactionData.loc[transactionData['merchantCategoryCode'] == 'gym', 'gMerchantCategoryCodeRiskTable'] = 3.567859601
    transactionData.loc[transactionData['merchantCategoryCode'] == 'health', 'gMerchantCategoryCodeRiskTable'] = 1.209004407
    transactionData.loc[transactionData['merchantCategoryCode'] == 'hotels', 'gMerchantCategoryCodeRiskTable'] = 0.771717107
    transactionData.loc[transactionData['merchantCategoryCode'] == 'mobileapps', 'gMerchantCategoryCodeRiskTable'] = 5.482702989
    transactionData.loc[transactionData['merchantCategoryCode'] == 'online_gifts', 'gMerchantCategoryCodeRiskTable'] = -0.438095045
    transactionData.loc[transactionData['merchantCategoryCode'] == 'online_retail', 'gMerchantCategoryCodeRiskTable'] = -0.445288768
    transactionData.loc[transactionData['merchantCategoryCode'] == 'online_subscriptions', 'gMerchantCategoryCodeRiskTable'] = 5.179287384
    transactionData.loc[transactionData['merchantCategoryCode'] == 'personal care', 'gMerchantCategoryCodeRiskTable'] = 1.247408781
    transactionData.loc[transactionData['merchantCategoryCode'] == 'rideshare', 'gMerchantCategoryCodeRiskTable'] = -0.464512622
    transactionData.loc[transactionData['merchantCategoryCode'] == 'subscriptions', 'gMerchantCategoryCodeRiskTable'] = 0.517126236

    # Todo: Add features that require previous transaction knowledge. Current data is too sparse

    # Write out dataset
    transactionData.to_json(outputFile, orient="records", lines=True)

    with open(outputResults, 'w') as output:
        print('Confirming...')
        print('Input file is: ' + inputFile)
        print('Output file is: ' + outputFile)
        print('Output results are found here: ' + outputResults)

        print("--------Data Information---------", file=output)
        print(transactionData.info(buf=output))

        print("--------Number of Unique Customers---------", file=output)
        print(transactionData['customerId'].nunique(), file=output)

        print("--------Frequency of Partition---------", file=output)
        print(transactionData['gPartition'].value_counts().sort_index(0), file=output)

        print("--------Frequency of Number of Days Since Last Address Change---------", file=output)
        print(transactionData['gNumDaysSinceLastAddressChange'].value_counts().sort_index(0), file=output)

        print("--------Frequency of Indicator of Address Change in Last 30 Days---------", file=output)
        print(transactionData['gIndLastAddressChangeWithin30Days'].value_counts().sort_index(0), file=output)

        print("--------Frequency of Number of Months Open---------", file=output)
        print(transactionData['gNumMonthsOpen'].value_counts().sort_index(0), file=output)

        print("--------Frequency of Merchant Category Code Risk Table---------", file=output)
        print(transactionData['gMerchantCategoryCodeRiskTable'].value_counts().sort_index(0), file=output)

        print("--------Cross Tab of Merchant Category Code Risk Table with MCC---------", file=output)
        print(pd.crosstab(transactionData['gMerchantCategoryCodeRiskTable'], transactionData['merchantCategoryCode']), file=output)

    output.close()


# Create infile and outfile
(inputFile, outputFile, outputResults) = readArgs(sys.argv[1:])

# Execute generateFrequenciesHistograms with the passed in arguments
addFeatures(inputFile, outputFile, outputResults)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)
