## IMPORT MODULES
import sqlite3
import mod_9_1_WriteCSVFileFromDB as mod_91

## DECLARE VARIABLES
DBPath = "data.db"

## TABLE NAMES IN ORDER
TableNames = [
    "FamilyOffices",
    "WealthManagement",
    "Endowments",
    "InstitutionalInvestment",
    "InvestmentBanking",
    "PrivateBanks",
    "MerchantBanks",
    "PensionFunds",
    "FundOfFund"
]

## FILEPATHS FOR CSV FILES - OUTPUT
FilePathsOutput = [
    "DATA_OUTPUT/DB_1_FROM_DB_FamilyOffices.csv",
    "DATA_OUTPUT/DB_2_FROM_DB_WealthManagement.csv",
    "DATA_OUTPUT/DB_3_FROM_DB_Endowments.csv",
    "DATA_OUTPUT/DB_4_FROM_DB_InstitutionalInvestment.csv",
    "DATA_OUTPUT/DB_5_FROM_DB_InvestmentBanking.csv",
    "DATA_OUTPUT/DB_6_FROM_DB_PrivateBanks.csv",
    "DATA_OUTPUT/DB_7_FROM_DB_MerchantBanks.csv",
    "DATA_OUTPUT/DB_8_FROM_DB_PensionFunds.csv",
    "DATA_OUTPUT/DB_9_FROM_DB_FundOfFund.csv"
]

## BEGIN DEFINE FUNCTION
def fn_WriteAllCSVFilesFromDB():
    ## EXPORT ALL TABLES FROM DB TO CSV FILES
    
    ## BUILD REPORT
    Report = "\n" + "="*60 + "\n"
    Report += "CSV EXPORT REPORT\n"
    Report += "="*60 + "\n\n"
    
    ## EXPORT EACH TABLE WITH CORRESPONDING FILE PATH
    for TableName, OutputFilePath in zip(TableNames, FilePathsOutput):
        Result = mod_91.fn_WriteCSVFileFromDB(TableName, OutputFilePath)
        Report += f"{Result}\n"
    
    ## FINAL STATUS
    Report += "\n" + "="*60 + "\n"
    Report += f"EXPORT COMPLETED: {len(TableNames)} tables exported\n"
    Report += "="*60 + "\n"
    
    ## RETURN REPORT
    return Report
## END DEFINE FUNCTION