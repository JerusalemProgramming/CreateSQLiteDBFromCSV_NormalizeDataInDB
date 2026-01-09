## IMPORT MODULES
import sqlite3
import csv
import os

## DECLARE VARIABLES
DBPath = "data.db"

## BEGIN DEFINE FUNCTION
def fn_WriteCSVFileFromDB(TableName, OutputFilePath):
    ## EXPORT SINGLE TABLE FROM DB TO CSV FILE WITH ORDERED COLUMNS
    
    ## CREATE OUTPUT DIRECTORY IF NOT EXISTS
    OutputDir = os.path.dirname(OutputFilePath)
    if OutputDir and not os.path.exists(OutputDir):
        os.makedirs(OutputDir)
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## GET ALL COLUMN NAMES FROM TABLE
    Cursor.execute(f"PRAGMA table_info({TableName})")
    AllColumns = [Col[1] for Col in Cursor.fetchall()]
    
    ## DEFINE PRENORMALIZE FIELD MAPPINGS
    PreNormalizeFields = {
        "FirmName": "PreNormalize_FirmName",
        "AddressOfCompany": "PreNormalize_AddressOfCompany",
        "City": "PreNormalize_City",
        "State_Province": "PreNormalize_State_Province",
        "PostalZipCode": "PreNormalize_PostalZipCode",
        "Country": "PreNormalize_Country",
        "Website": "PreNormalize_Website",
        "EmailOfCompany": "PreNormalize_EmailOfCompany",
        "ContactName_First": "PreNormalize_ContactName_First",
        "ContactName_Last": "PreNormalize_ContactName_Last",
        "Contact_TitlePosition": "PreNormalize_Contact_TitlePosition",
        "EmailOfContact": "PreNormalize_EmailOfContact",
        "NumberPhone": "PreNormalize_NumberPhone",
        "NumberFax": "PreNormalize_NumberFax",
        "LinkedInProfile": "PreNormalize_LinkedInProfile"
    }
    
    ## BUILD ORDERED COLUMN LIST
    OrderedColumns = []
    PreNormalizeColumnsUsed = set()
    
    for Col in AllColumns:
        ## ADD ORIGINAL COLUMN
        if not Col.startswith("PreNormalize_"):
            OrderedColumns.append(Col)
            
            ## CHECK IF PRENORMALIZE COLUMN EXISTS FOR THIS FIELD
            if Col in PreNormalizeFields:
                PreNormalizeCol = PreNormalizeFields[Col]
                if PreNormalizeCol in AllColumns:
                    OrderedColumns.append(PreNormalizeCol)
                    PreNormalizeColumnsUsed.add(PreNormalizeCol)
    
    ## ADD ANY REMAINING PRENORMALIZE COLUMNS NOT YET ADDED
    for Col in AllColumns:
        if Col.startswith("PreNormalize_") and Col not in PreNormalizeColumnsUsed:
            OrderedColumns.append(Col)
    
    ## BUILD SELECT STATEMENT WITH ORDERED COLUMNS
    ColumnList = ", ".join([f'"{Col}"' for Col in OrderedColumns])
    Cursor.execute(f"SELECT {ColumnList} FROM {TableName}")
    Rows = Cursor.fetchall()
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## WRITE TO CSV
    with open(OutputFilePath, 'w', newline='', encoding='utf-8') as CsvFile:
        Writer = csv.writer(CsvFile, delimiter=';')
        
        ## WRITE HEADER
        Writer.writerow(OrderedColumns)
        
        ## WRITE DATA ROWS
        Writer.writerows(Rows)
    
    ## RETURN RESULT
    return f"Exported {len(Rows)} rows to {OutputFilePath}"
## END DEFINE FUNCTION