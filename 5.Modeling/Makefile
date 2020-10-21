runExploreRawFieldCorrelationWithTag:
	./ExploreRawFieldCorrelationWithTag.py --inputPath '/home/shell/Data/CapOneDSChallenge/DS/' --dsName 'transactions' --dsNameExtension '.withKey.txt'

runAddFeatures:
	./AddFeatures.py --inputPath '/home/shell/Data/CapOneDSChallenge/DS/' --dsName 'transactions' --dsNameExtension '.duplicatesIdentified.txt'

runCreateModelingDataset:
	./CreateModelingDataset.py --inputPath '/home/shell/Data/CapOneDSChallenge/DS/' --dsName 'transactions' --dsNameExtension '.withFeatures.txt'

runExploreAllFieldCorrelationWithTag:
	./ExploreAllFieldCorrelationWithTag.py --inputPath '/home/shell/Data/CapOneDSChallenge/DS/' --dsName 'transactions' --dsNameExtension '.modelingPopulation.txt'

runBuildSvmModel:
    ./BuildSvmModel.py
