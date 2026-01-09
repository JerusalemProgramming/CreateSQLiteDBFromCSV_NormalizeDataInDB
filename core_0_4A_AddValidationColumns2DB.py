## IMPORT MODULES
import sqlite3

## DEFINE FUNCTION
def fn_AddValidationColumns2DB():
    ## DECLARE VARIABLES
    DBPath = "data.db"
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
    
    ## DEFINE VALIDATION COLUMNS TO ADD
    Columns = [
        "ValidationSyntax INTEGER DEFAULT NULL",
        "ValidationDomainExists INTEGER DEFAULT NULL",
        "ValidationMxRecords INTEGER DEFAULT NULL",
        "ValidationMailboxExists INTEGER DEFAULT NULL",
        "ValidationIsDisposable INTEGER DEFAULT NULL",
        "ValidationIsRoleBased INTEGER DEFAULT NULL",
        "ValidationScore INTEGER DEFAULT NULL",
        "ValidationStatus TEXT DEFAULT NULL",
        "ValidationDateTime TEXT DEFAULT NULL",
        "ValidationAttempts INTEGER DEFAULT NULL",
        "ValidationErrorMessage TEXT DEFAULT NULL"
    ]
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## BUILD REPORT
    Report = "\n" + "="*60 + "\n"
    Report += "ADDING EMAIL VALIDATION COLUMNS TO DATABASE\n"
    Report += "="*60 + "\n\n"
    
    TotalColumnsAdded = 0
    TotalColumnsSkipped = 0
    TotalErrors = 0

    ## LOOP THROUGH EACH TABLE
    for TableName in TableNames:
        
        Report += f"TABLE: {TableName}\n"
        
        TableColumnsAdded = 0
        TableColumnsSkipped = 0
        TableErrors = 0

        ## LOOP THROUGH EACH COLUMN
        for Column in Columns:
            
            ColumnName = Column.split()[0]
        
            try:
        
                ## ADD COLUMN TO TABLE
                Query = f"ALTER TABLE {TableName} ADD COLUMN {Column}"
                Cursor.execute(Query)
                Report += f"  ✓ Added: {ColumnName}\n"
                TableColumnsAdded += 1
                TotalColumnsAdded += 1
        
            except sqlite3.OperationalError as E:
        
                ## COLUMN ALREADY EXISTS
                if "duplicate column name" in str(E).lower():
                    Report += f"  - Skipped (already exists): {ColumnName}\n"
                    TableColumnsSkipped += 1
                    TotalColumnsSkipped += 1
                else:
                    ## OTHER ERROR
                    Report += f"  ✗ Error: {ColumnName} - {E}\n"
                    TableErrors += 1
                    TotalErrors += 1
        
        ## TABLE SUMMARY
        Report += f"  Summary: {TableColumnsAdded} added, {TableColumnsSkipped} skipped, {TableErrors} errors\n"
        Report += "-"*60 + "\n\n"
    
    ## COMMIT CHANGES AND CLOSE CONNECTION
    Connection.commit()
    Connection.close()
    
    ## OVERALL SUMMARY
    Report += "="*60 + "\n"
    Report += "OVERALL SUMMARY:\n"
    Report += f"  Total columns added: {TotalColumnsAdded}\n"
    Report += f"  Total columns skipped: {TotalColumnsSkipped}\n"
    Report += f"  Total errors: {TotalErrors}\n"
    
    if TotalErrors == 0:
        Report += "\nSTATUS: Email validation columns added successfully ✓\n"
    else:
        Report += f"\nSTATUS: Completed with {TotalErrors} error(s) ✗\n"
    
    Report += "="*60 + "\n"
    
    ## RETURN REPORT
    return Report