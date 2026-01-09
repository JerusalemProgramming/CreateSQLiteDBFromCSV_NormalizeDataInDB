## IMPORT MODULES
import sqlite3

## DEFINE FUNCTION
def fn_VerifyValidationColumnsInDB():
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
    
    ## EXPECTED VALIDATION COLUMNS
    ExpectedValidationColumns = [
        "ValidationSyntax",
        "ValidationDomainExists",
        "ValidationMxRecords",
        "ValidationMailboxExists",
        "ValidationIsDisposable",
        "ValidationIsRoleBased",
        "ValidationScore",
        "ValidationStatus",
        "ValidationDateTime",
        "ValidationAttempts",
        "ValidationErrorMessage"
    ]
    
    ## COUNT EXPECTED COLUMNS
    TotalExpectedColumns = len(ExpectedValidationColumns)
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## BUILD REPORT
    Report = "\n" + "="*60 + "\n"
    Report += "EMAIL VALIDATION COLUMNS VERIFICATION REPORT\n"
    Report += "="*60 + "\n\n"
    
    AllTablesValid = True
    TotalPresentColumns = 0
    TotalMissingColumns = 0
    
    ## LOOP THROUGH EACH TABLE
    for TableName in TableNames:
        ## GET TABLE SCHEMA
        Cursor.execute(f"PRAGMA table_info({TableName})")
        Columns = Cursor.fetchall()
        
        ## EXTRACT COLUMN NAMES
        ColumnNames = [Col[1] for Col in Columns]
        
        ## CHECK FOR VALIDATION COLUMNS
        Report += f"TABLE: {TableName}\n"
        Report += f"Total Columns: {len(ColumnNames)}\n"
        
        MissingColumns = []
        PresentColumns = []
        
        for ValidationCol in ExpectedValidationColumns:
            if ValidationCol in ColumnNames:
                PresentColumns.append(ValidationCol)
                Report += f"  ✓ {ValidationCol}\n"
            else:
                MissingColumns.append(ValidationCol)
                Report += f"  ✗ {ValidationCol} (MISSING)\n"
                AllTablesValid = False
        
        TotalPresentColumns += len(PresentColumns)
        TotalMissingColumns += len(MissingColumns)
        
        Report += f"Validation Columns Present: {len(PresentColumns)}/{TotalExpectedColumns}\n"
        
        if MissingColumns:
            Report += f"STATUS: Missing {len(MissingColumns)} column(s) ✗\n"
        else:
            Report += "STATUS: All validation columns present ✓\n"
        
        Report += "-"*60 + "\n\n"
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## OVERALL SUMMARY
    Report += "="*60 + "\n"
    Report += "OVERALL SUMMARY:\n"
    Report += f"  Total tables checked: {len(TableNames)}\n"
    Report += f"  Total columns present: {TotalPresentColumns}/{len(TableNames) * TotalExpectedColumns}\n"
    Report += f"  Total columns missing: {TotalMissingColumns}\n\n"
    
    ## FINAL STATUS
    if AllTablesValid:
        Report += "OVERALL STATUS: All tables have all validation columns ✓\n"
    else:
        Report += "OVERALL STATUS: Some tables are missing validation columns ✗\n"
    
    Report += "="*60 + "\n"
    
    ## RETURN REPORT
    return Report