#! /usr/bin/python4
# Import statements
import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn import svm
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn import metrics

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(6)

FEATURES = ['availableMoney',
            'gTransactionHour',
            'availableMoney',
            'gNumMonthsOpen',
            'cardPresent',
            'currentBalance',
            'gIndLastAddressChangeWithin30Days',
            'transactionAmount',
            'gMCCRiskTable'
            ]


def Build_Data_Set():
    transactionData = pd.read_json('/home/shell/Data/CapOneDSChallenge/DS/transactions.withFraudInd.txt', lines=True)
    #                               nrows=1000)

    transactionData['gMCCRiskTable'] = transactionData['gMerchantCategoryCodeRiskTable'].replace(np.nan, 1)
    transactionData['gDT'] = pd.to_datetime(transactionData['transactionDateTime'])
    transactionData['gTransactionHour'] = transactionData['gDT'].dt.hour

    #transactionData = transactionData[:100]

    X = np.array(transactionData[FEATURES].values)
    #X = np.array(transactionData[FEATURES].values).tolist())

    y = (transactionData["isFraud"]
         #.replace("underperform",0)
         #.replace("outperform",1)
         .values.tolist())

    X = preprocessing.scale(X)

    return X,y


def Analysis():
    #test_size = 1000
    X, y = Build_Data_Set()
    print(len(X))

    clf = svm.SVC(kernel="linear", C=1.0)
    clf.fit(X, y)
    #clf.fit(X[:-test_size], y[:-test_size])

    #correct_count = 0

    #for x in range(1, test_size + 1):
    #    if clf.predict(X[-x])[0] == y[-x]:
    #        correct_count += 1

    #print("Accuracy:", (correct_count / test_size) * 100.00)


Analysis()

#print(transactionData.data.shape)

#X = transactionData[['availableMoney']]
#X = transactionData.data
#transactionData['isFraud'] = transactionData.target

#transactionData.X_normalized = preprocessing.normalize(X)
#transactionData.X_standardized = preprocessing.scale(X)


#print("No mod")
#print(transactionData.X.value_counts().sort_index(0))
#print("Normalized")
#print(transactionData.X_normalized.value_counts().sort_index(0))
#print("Standardized")
#print(transactionData.X_standardized.value_counts().sort_index(0))

