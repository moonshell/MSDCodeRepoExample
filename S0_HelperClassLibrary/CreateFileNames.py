def createInputFileName(inputPath, dsName, dsNameExtension):
    # Detail inputFile, outputFile, and outputResults strings
    inputFile = str(inputPath + dsName + '.' + dsNameExtension)
    print("Input file is: " + inputFile)
    return inputFile

def createOutputFileName(inputPath, dsName, dsNameExtension, additionalDescriptor, conversion=1):
    if conversion == 1:
        outputFile = str(inputPath + dsName + '.jsonl')
    elif conversion == 0:
        outputFile = str(inputPath + dsName + '.' + additionalDescriptor + '.' + dsNameExtension)
    print("Output file is: " + outputFile)
    return outputFile

def createOutputResultsName(dsName, additionalDescriptor):
    outputResults = 'Output/Output.' + dsName + '.' + additionalDescriptor + '.txt'
    print("Output results are found here: " + outputResults)
    return outputResults