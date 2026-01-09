## IMPORT MODULES
import mod_9_0_WriteCSVFileFromCSV as mod_90 ## WRITE CSV FILES

## DECLARE VARIABLES
## FILEPATHS FOR CSV FILES - OUTPUT
FilePathsOutput = [
    "DATA_OUTPUT/DB_1_FROM_INPUT_CSV_FamilyOffices.csv",
    "DATA_OUTPUT/DB_2_FROM_INPUT_CSV_WealthManagement.csv",
    "DATA_OUTPUT/DB_3_FROM_INPUT_CSV_Endowments.csv",
    "DATA_OUTPUT/DB_4_FROM_INPUT_CSV_InstitutionalInvestment.csv",
    "DATA_OUTPUT/DB_5_FROM_INPUT_CSV_InvestmentBanking.csv",
    "DATA_OUTPUT/DB_6_FROM_INPUT_CSV_PrivateBanks.csv",
    "DATA_OUTPUT/DB_7_FROM_INPUT_CSV_MerchantBanks.csv",
    "DATA_OUTPUT/DB_8_FROM_INPUT_CSV_PensionFunds.csv",
    "DATA_OUTPUT/DB_9_FROM_INPUT_CSV_FundOfFund.csv"
]

## BEGIN DEFINE FUNCTION
def fn_WriteAllCSVFilesFromCSV(ListOfPythonDicts):
    
    ## WRITE ALL DATA TO OUTPUT CSV FILES
    ## BEGIN WRITE CSV FILES
    ## BEGIN FOR LOOP
    for i in range(len(ListOfPythonDicts)):

        ## CALL MODULE.FUNCTION() - OPEN AND WRITE CSV FILE - ALL FILES
        _ = mod_90.fn_WriteCSVFileFromCSV(ListOfPythonDicts[i], FilePathsOutput[i])

    ## END FOR LOOP
    ## END WRITE CSV FILES

## END DEFINE FUNCTION





