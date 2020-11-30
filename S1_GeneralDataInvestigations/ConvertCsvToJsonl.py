#! /usr/bin/python3
# Import statements
import os
import sys
import pandas as pd
from datetime import datetime
DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, DIR)
from S0_HelperClassLibrary.ReadInArgs import readArgs
from S0_HelperClassLibrary.CreateFileNames import *

"""ConvertCvsToJsonl.py is a script to convert a dataset from CSV to JSONL for further processing.

This script is the first script in General Data Investigations.  It is used to read in a csv dataset and convert to a 
json lines dataset.  The output results are saved in the same path as the input data, and named the same as the input
file with a .jsonl extension.
"""
print(__doc__)

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(5)

# Generate Summary Statistics: Info, Shape, Description, Head
def readCsvConvertToJsonl(inputPath, dsName, dsNameExtension):
    # Detail inputFile, outputFile, and outputResults strings
    inputFile = createInputFileName(inputPath, dsName, dsNameExtension)
    outputFile = createOutputFileName(inputPath, dsName, dsNameExtension, "none", conversion=1)
    outputResults = createOutputResultsName(dsName, "ConvertCsvToJsonl")

    # Increase size of columns displayed in output file
    pd.set_option("display.max_columns", 500)
    # Read in data with chunksize to reduce CPU needs
    chunkedData = pd.read_csv(inputFile, chunksize=100000)
    # Append chunked data into 1 data frame
    fullData = pd.concat(chunkedData, ignore_index=True)


    with open(outputResults, 'w') as output:
        # Print the head of the csv file
        print("First records of CSV", file=output)
        print(fullData.head(), file=output)

        # Convert to jsonl
        fullData.to_json (outputFile, orient='records', lines=True)

        # Print the head of the jsonl file
        fullConvertedData = pd.read_json(outputFile, lines=True)

        print("First records of JSONL", file=output)
        print(fullConvertedData.head(), file=output)
    output.close()


# Create infile and outfile
(inputPath, dsName, dsNameExtension) = readArgs(sys.argv[1:])

# Execute generateSummaryStats with the passed in arguments
readCsvConvertToJsonl(inputPath, dsName, dsNameExtension)

# Capture end time and print out run time
endTime = datetime.now()
print(endTime - startTime)