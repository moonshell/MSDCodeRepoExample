runAddFeatures:
	./AddFeatures.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension '.duplicatesIdentified.txt'

runCreateModelingDataset:
	./CreateModelingDataset.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension '.withFeatures.txt'

runProduceCorrelationMatrixWithTag:
	./ProduceCorrelationMatrixWithTag.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension '.modelingPopulation.txt' --correlationMethod 'pearson'