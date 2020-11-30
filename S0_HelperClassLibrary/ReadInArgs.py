#! /usr/bin/python3
import sys
import getopt

# Read in parameters
def readArgs(argv):
    """
    Run script with following command:
        <scriptName.py> -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

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
    inputPath : str
        The path of the input dataset.
    dsName : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt

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

    return inputPath, dsName, dsNameExtension


def readArgs2Inputs(argv):
    """
    Run script with following command:
        <scriptName.py> -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension>

    Parameters
    __________
    inputPath : str
        The path of the input dataset.
    dsName1 : str
        The name of the dataset without it's extension.
    dsName2 : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt
    matchingField : str
        The name of the dataset without it's extension.

    Return
    _______
    inputPath : str
        The path of the input dataset.
    dsName1 : str
        The name of the dataset without it's extension.
    dsName2 : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt
    matchingField : str
        The name of the dataset without it's extension.

    """
    inputPath = ''
    dsName1 = ''
    dsName2 = ''
    dsNameExtension = ''
    matchingField = ''

    # Three parameters are expected and h for help is also available
    try:
        opts, args = getopt.getopt(argv, "h", ["inputPath=", "dsName1=", "dsName2=", "dsNameExtension=", "matchingField="])
        print("The input arguments are: ", opts, args)
    except getopt.GetoptError as error:
        print(readArgs2Inputs.__doc__)
        print("Need passed parameters.  Code did not run.")
        sys.exit()

    # Set values for each parameter with the contents of the argument
    for opt, arg in opts:
        if opt in '-h':
            print(readArgs2Inputs.__doc__)
            sys.exit()
            print("Need passed parameters.  Code did not run.")
        elif opt in '--inputPath':
            inputPath = arg
        elif opt in '--dsName1':
            dsName1 = arg
        elif opt in '--dsName2':
            dsName2 = arg
        elif opt in '--dsNameExtension':
            dsNameExtension = arg
        elif opt in '--matchingField':
            matchingField = arg

    return inputPath, dsName1, dsName2, dsNameExtension, matchingField


def readArgsWithCorrelationAndTag(argv):
    """
    Run script with following command:
        <scriptName.py> -inputPath <path> -dsName <filename without extension> -dsNameExtension <extension> -correlationMethod <method> -targetField <tag>

    Parameters
    __________
    inputPath : str
        The path of the input dataset.
    dsName : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: txt
    correlationMethod : str
        The correlation method to be used. Example: Pearson
    targetField : str
        The field associated with the target.  Example: isFraud

    Return
    _______
    inputPath : str
        The path of the input dataset.
    dsName : str
        The name of the dataset without it's extension.
    dsNameExtension : str
        The extension of the dataset name.  Example: .txt
    correlationMethod : str
        The correlation method to be used. Example: Pearson
    targetField : str
        The field associated with the target.  Example: .txt
    """
    inputPath = ''
    dsName = ''
    dsNameExtension = ''
    correlationMethod = ''
    targetField = ''

    # Three parameters are expected and h for help is also available
    try:
        opts, args = getopt.getopt(argv, "h", ["inputPath=", "dsName=", "dsNameExtension=", "correlationMethod=", "targetField="])
        print("The input arguments are: ", opts, args)
    except getopt.GetoptError as error:
        print(readArgsWithCorrelationAndTag.__doc__)
        print("Need passed parameters.  Code did not run.")
        sys.exit()

    # Set values for each parameter with the contents of the argument
    for opt, arg in opts:
        if opt in '-h':
            print(readArgsWithCorrelationAndTag.__doc__)
            sys.exit()
            print("Need passed parameters.  Code did not run.")
        elif opt in '--inputPath':
            inputPath = arg
        elif opt in '--dsName':
            dsName = arg
        elif opt in '--dsNameExtension':
            dsNameExtension = arg
        elif opt in '--correlationMethod':
            correlationMethod = arg
        elif opt in '--targetField':
            targetField = arg

    return inputPath, dsName, dsNameExtension, correlationMethod, targetField