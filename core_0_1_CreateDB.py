## IMPORT MODULES
import sqlite3

## DECLARE VARIABLES
## DATABASE FILE PATH
DBPath = "data.db"

## TABLE NAMES
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

## BEGIN DEFINE FUNCTION
def fn_CreateDB(ListOfPythonDicts):
    ## CREATE SQLITE DATABASE WITH 9 TABLES FROM PYTHON DICTS
    ## COMPANY-ONLY ROWS ARE NOW CREATED IN READCSVFILE MODULES

    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()

    print(f"\nCREATING DATABASE")
    
    ## LOOP THROUGH EACH DICT AND CREATE CORRESPONDING TABLE
    for i in range(len(ListOfPythonDicts)):
        
        TableName = TableNames[i]
        DataDict = ListOfPythonDicts[i]
        ## TEST PRINT OUTPUT - DEBUG
        print(f"Processing {TableName}: {len(DataDict)} offices")
        
        ## DROP TABLE IF EXISTS
        Cursor.execute(f"DROP TABLE IF EXISTS {TableName}")
        
        ## CHECK IF DATADICT HAS ANY OFFICES
        if len(DataDict) > 0:
            ## GET FIRST OFFICE'S FIRST CONTACT TO EXTRACT COLUMN NAMES
            FirstOfficeKey = list(DataDict.keys())[0]
            FirstContactList = DataDict[FirstOfficeKey]
            
            if len(FirstContactList) > 0:
                ColumnNames = list(FirstContactList[0].keys())
                
                ## CREATE COLUMN DEFINITIONS (ALL TEXT TYPE)
                ColumnDefs = ", ".join([f'"{Col}" TEXT' for Col in ColumnNames])
                
                ## CREATE TABLE
                CreateTableSQL = f"CREATE TABLE {TableName} ({ColumnDefs})"
                Cursor.execute(CreateTableSQL)
                
                ## INSERT DATA FROM ALL OFFICES
                PlaceholderSQL = ", ".join(["?" for _ in ColumnNames])
                InsertSQL = f'INSERT INTO {TableName} VALUES ({PlaceholderSQL})'
                
                ## LOOP THROUGH ALL OFFICES AND THEIR CONTACTS
                for OfficeNumber, ContactsList in DataDict.items():
                    
                    ## INSERT ALL CONTACT ROWS (INCLUDING COMPANY-ONLY ROWS FROM READCSVFILE)
                    for Contact in ContactsList:
                        Values = [Contact.get(Col, "") for Col in ColumnNames]
                        Cursor.execute(InsertSQL, Values)
    
    ## COMMIT AND CLOSE
    Connection.commit()
    Connection.close()
    
    ## RETURN SUCCESS MESSAGE
    return f"Database created successfully at {DBPath} with {len(TableNames)} tables"
## END DEFINE FUNCTION