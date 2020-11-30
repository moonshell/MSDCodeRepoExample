def datasetShapeInfo(dsName, inputFile, outputResults):
    with open(outputResults, 'w') as output:
        print('Input file is: ' + inputFile, file=output)
        print('Output results are found here: ' + outputResults, file=output)
        print("---------File Info--------", file=output)
        print(dsName.info(buf=output))
        print("File Info Complete")
        print("---------File Shape--------", file=output)
        print(dsName.shape, file=output)
        print("File Shape Complete")
    output.close()

    return


def datasetSummary(dsName, inputFile, outputResults):
    with open(outputResults, 'w') as output:
        print('Input file is: ' + inputFile, file=output)
        print('Output results are found here: ' + outputResults, file=output)
        print("---------File Info--------", file=output)
        print(dsName.info(buf=output))
        print("File Info Complete")
        print("---------File Shape--------", file=output)
        print(dsName.shape, file=output)
        print("File Shape Complete")
        print("---------File Description of Fields--------", file=output)
        print(dsName.describe(), file=output)
        print("File Description Complete")
        print("---------File Header--------", file=output)
        print(dsName.head(), file=output)
        print("File Header Complete")
    output.close()

    return
