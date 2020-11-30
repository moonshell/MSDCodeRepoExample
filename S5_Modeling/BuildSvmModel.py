#! /usr/bin/python4
# Import statements
import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn import svm
from sklearn.model_selection import train_test_split
from sklearn import metrics

# Start time of execution of the script
startTime = datetime.now()

# Priority on the server
os.nice(6)

transactionData = pd.read_json('/home/shell/Data/CapOneDSChallenge/DS/transactions.withFraudInd.txt', lines=True)
#transactionData = pd.read_json('/home/shell/Data/CapOneDSChallenge/DS/transactions.modelingPopulation.txt', lines=True)
transactionData['gMCCRiskTable'] = transactionData['gMerchantCategoryCodeRiskTable'].replace(np.nan, 1)
transactionData['gDT'] = pd.to_datetime(transactionData['transactionDateTime'])
transactionData['gTransactionHour'] = transactionData['gDT'].dt.hour

#print(transactionData['gMerchantCategoryCodeRiskTable_Rounded'].value_counts().sort_index(1))

#Model iteration
X8 = transactionData[['gTransactionHour', 'availableMoney', 'gNumMonthsOpen', 'cardPresent', 'currentBalance', 'gIndLastAddressChangeWithin30Days', 'transactionAmount', 'gMCCRiskTable']]
X7 = transactionData[['availableMoney', 'gNumMonthsOpen', 'cardPresent', 'currentBalance', 'gIndLastAddressChangeWithin30Days', 'transactionAmount', 'gMCCRiskTable']]
X6 = transactionData[['cardPresent', 'currentBalance', 'gIndLastAddressChangeWithin30Days', 'transactionAmount', 'gMCCRiskTable']]
X5 = transactionData[['currentBalance', 'gIndLastAddressChangeWithin30Days', 'transactionAmount', 'gMCCRiskTable']]
X4 = transactionData[['gIndLastAddressChangeWithin30Days', 'transactionAmount', 'gMCCRiskTable']]
X3 = transactionData[['transactionAmount', 'gMCCRiskTable']]
X1 = transactionData[['gMCCRiskTable']]
#X = transactionData[['gIndCardCvvEqEnteredCVV', 'gIndLastAddressChangeWithin30Days', 'gNumMonthsOpen', 'gMerchantCategoryCodeRiskTable']]

X = X7
y = transactionData['isFraud']

# Create Train-Test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=109)
clf = svm.SVC(kernel='rbf', probability=True)

#clf.fit(modelingData.data, modelingData.target)
clf.fit(X_train, y_train)

y_pred = clf.predict(X_test)

with open('Output/Model.log', 'a+') as output:
    print('--------RUN-------', file=output)
    #print(clf.coef_, file=output)

    # Model Accuracy on Validation set
    print("Accuracy: ", metrics.accuracy_score(y_test, y_pred), file=output)

    # Model Precision on Validation set
    print("Precision:", metrics.precision_score(y_test, y_pred), file=output)

    # Model Recall on Validation set
    print("Recall:", metrics.recall_score(y_test, y_pred), file=output)

# Capture end time and print out run time
endTime = datetime.now()

print(endTime - startTime)
