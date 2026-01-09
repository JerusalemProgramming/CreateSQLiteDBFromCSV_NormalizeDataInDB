## IMPORT MODULES
## INPUT: READ CSV FILES
## IMPORT ALL CSV READING MODULES
import mod_0_1_ReadCSVFile as mod_01 ## FAMILY OFFICES
import mod_0_2_ReadCSVFile as mod_02 ## WEALTH MANAGEMENT
import mod_0_3_ReadCSVFile as mod_03 ## ENDOWMENTS
import mod_0_4_ReadCSVFile as mod_04 ## INSTITUTIONAL INVESTMENT
import mod_0_5_ReadCSVFile as mod_05 ## INVESTMENT BANKING
import mod_0_6_ReadCSVFile as mod_06 ## PRIVATE BANKS
import mod_0_7_ReadCSVFile as mod_07 ## MERCHANT BANKS
import mod_0_8_ReadCSVFile as mod_08 ## PENSION FUNDS
import mod_0_9_ReadCSVFile as mod_09 ## FUND OF FUND

## DECLARE VARIABLES
## FILEPATHS FOR CSV FILES - INPUT
FilePathsInput = [
    "DATA_INPUT/DB_1_FamilyOffices.csv",
    "DATA_INPUT/DB_2_WealthManagement.csv",
    "DATA_INPUT/DB_3_Endowments.csv",
    "DATA_INPUT/DB_4_InstitutionalInvestment.csv",
    "DATA_INPUT/DB_5_InvestmentBanking.csv",
    "DATA_INPUT/DB_6_PrivateBanks.csv",
    "DATA_INPUT/DB_7_MerchantBanks.csv",
    "DATA_INPUT/DB_8_PensionFunds.csv",
    "DATA_INPUT/DB_9_FundOfFund.csv"
]

## BEGIN DEFINE FUNCTION
def fn_ReadAllCSVFiles():

    # READ ALL 9 CSV FILES AND RETURN AS LIST OF DICTS
    DataFamilyOffices = mod_01.fn_ReadCSVFile(FilePathsInput[0])
    DataWealthManagement = mod_02.fn_ReadCSVFile(FilePathsInput[1])
    DataEndowments = mod_03.fn_ReadCSVFile(FilePathsInput[2])
    DataInstitutionalInvestment = mod_04.fn_ReadCSVFile(FilePathsInput[3])
    DataInvestmentBanking = mod_05.fn_ReadCSVFile(FilePathsInput[4])
    DataPrivateBanks = mod_06.fn_ReadCSVFile(FilePathsInput[5])
    DataMerchantBanks = mod_07.fn_ReadCSVFile(FilePathsInput[6])
    DataPensionFunds = mod_08.fn_ReadCSVFile(FilePathsInput[7])
    DataFundOfFund = mod_09.fn_ReadCSVFile(FilePathsInput[8])
    
    ## RETURN VARIABLES
    return(DataFamilyOffices, DataWealthManagement, DataEndowments, 
            DataInstitutionalInvestment, DataInvestmentBanking, DataPrivateBanks,
            DataMerchantBanks, DataPensionFunds, DataFundOfFund)

## END DEFINE FUNCTION
