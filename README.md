MSD Code Repo Example
-------------
This repo is not intended to be a complete analysis, rather a sample of my approach to coding.  

If you just wanted to look at a smaller sample of code, I recommend these three scripts:
* S1_GeneralDataInvestigations/GenerateFrequenciesHistograms.py
    * This highlights the calculations and exhibits produced on each field by iterating through each column.
* S3_InvestigateDuplicateTransactions/InvestigateAndTagDuplicates.py
    * This highlights the retention of information from the previous record using iterrows.
* S4_ModelingPrep/GenerateCorrelationMatrix.py
    * This highlights generalizations in the code, such as the correlation algorithm to utilize.

Quick summary of the code:
-------------
The code contains the aspects leading up to model development, including data investigations and some modeling dataset 
preparation steps.  This repository does not include any modeling or any output of the scripts. The code is structured 
in a way that a data scientist colleague can follow it. The directories contain a step number associated with the set of 
code, readmes, and makefiles. The script names are descriptive. All code and arguments are commented with docstrings and 
single line comments. Tracking of called scripts with the passed arguments are maintained in the Makefile.  For instance, a data scientist can run "make runSummaryStats" from the command line and get the same results as the initial run because the parameters are guaranteed to be exactly the same.

If I was instructing the team to improve/standardize the code, I would:
* Continue to improve generalization of the code.  For instance, if the data is not standardized to json line, one parameter can be the type of file (csv, jsonl) and the code can read in the file based on the type.  There is an example of generalization in the correlation code highlighted above.
* Add test cases especially when new variables/features are added to the dataset.
* Create additional methods/definitions library of repeated functions that can be used at multiple stages of the project and called from any script.
* Add evaluation of the test and train populations.

Set-up
-------------

For this project, I have used Python 3.8.

Packages Used:
 - numpy
 - pandas
 - math
 - Seaborn
 - Matplot lib
 - Scikit-learn
    
These are all available via pip to install. 

Each program is set up with parameters.  To get the parameters for any script type 'scriptname.py -h'.

In addition, each script has an associated makefile entry.  View the Makefile in any directory to see the commands that
were executed.

Without alteration, all makefile commands can be run on the command line with: "make < runScript >".