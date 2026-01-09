## IMPORT MODULES
import sqlite3

## DEFINE FUNCTION
def fn_VerifyTemplateColumnsInDB():
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
    
    ## EXPECTED TEMPLATE COLUMNS
    ExpectedTemplateColumns = [
        "OfficeNumber",
        "FirmName",
        "AddressOfCompany",
        "City",
        "State_Province",
        "PostalZipCode",
        "Website",
        "EmailOfCompany",
        "ContactName_First",
        "ContactName_Last",
        "Contact_TitlePosition",
        "NumberPhone",
        "EmailOfContact"
    ]
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## BUILD REPORT
    Report = "\n" + "="*60 + "\n"
    Report += "TEMPLATE COLUMNS VERIFICATION REPORT\n"
    Report += "="*60 + "\n\n"
    
    AllTablesValid = True
    
    ## LOOP THROUGH EACH TABLE
    for TableName in TableNames:
        ## GET TABLE SCHEMA
        Cursor.execute(f"PRAGMA table_info({TableName})")
        Columns = Cursor.fetchall()
        
        ## EXTRACT COLUMN NAMES
        ColumnNames = [Col[1] for Col in Columns]
        
        ## CHECK FOR TEMPLATE COLUMNS
        Report += f"TABLE: {TableName}\n"
        Report += f"Total Columns: {len(ColumnNames)}\n"
        
        MissingColumns = []
        PresentColumns = []
        
        for TemplateCol in ExpectedTemplateColumns:
            if TemplateCol in ColumnNames:
                PresentColumns.append(TemplateCol)
            else:
                MissingColumns.append(TemplateCol)
                AllTablesValid = False
        
        Report += f"Template Columns Present: {len(PresentColumns)}/13\n"
        
        if MissingColumns:
            Report += f"MISSING COLUMNS: {', '.join(MissingColumns)}\n"
        else:
            Report += "STATUS: All template columns present ✓\n"
        
        Report += "-"*60 + "\n\n"
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## FINAL STATUS
    if AllTablesValid:
        Report += "OVERALL STATUS: All tables have all template columns ✓\n"
    else:
        Report += "OVERALL STATUS: Some tables are missing template columns ✗\n"
    
    Report += "="*60 + "\n"
    
    ## RETURN REPORT
    return Report