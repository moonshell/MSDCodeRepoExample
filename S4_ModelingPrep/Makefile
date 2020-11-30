runAddFeatures:
	./AddFeatures.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension 'DuplicatesIdentified.jsonl'

runCreateModelingDataset:
	./CreateModelingDataset.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension 'withFeatures.jsonl'

runProduceCorrelationMatrixWithTag:
	./ProduceCorrelationMatrixWithTag.py --inputPath '/home/shell/Data/DS/' --dsName 'transactions' --dsNameExtension 'modelingPopulation.jsonl' --correlationMethod 'pearson' --targetField 'isFraud'