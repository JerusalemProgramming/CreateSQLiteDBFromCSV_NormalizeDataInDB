## IMPORT MODULES
import sqlite3

## DEFINE FUNCTION
def fn_AddNewColumns2DB():
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
    
    ## DEFINE COLUMNS TO ADD
    Columns = [
        "EmailSent INTEGER DEFAULT NULL",
        "EmailSentDateTime TEXT DEFAULT NULL",
        "EmailDelivered INTEGER DEFAULT NULL",
        "EmailDeliveredDateTime TEXT DEFAULT NULL",
        "EmailOpened INTEGER DEFAULT NULL",
        "EmailClicked INTEGER DEFAULT NULL",
        "EmailBounced INTEGER DEFAULT NULL",
        "EmailRejected INTEGER DEFAULT NULL",
        "EmailDropped INTEGER DEFAULT NULL",
        "EmailComplained INTEGER DEFAULT NULL",
        "EmailUnsubscribed INTEGER DEFAULT NULL",
        "EmailStatus TEXT DEFAULT NULL",
        "EmailFailureReason TEXT DEFAULT NULL",
        "EmailFailureSeverity TEXT DEFAULT NULL",
        "EmailAttempts INTEGER DEFAULT NULL",
        "EmailAcceptedDateTime TEXT DEFAULT NULL",
        "MailgunMessageID TEXT DEFAULT NULL",
        "EmailSentToContact INTEGER DEFAULT NULL",
        "EmailSentToCompany INTEGER DEFAULT NULL",
        "PreNormalize_FirmName TEXT DEFAULT NULL",
        "PreNormalize_AddressOfCompany TEXT DEFAULT NULL",
        "PreNormalize_City TEXT DEFAULT NULL",
        "PreNormalize_State_Province TEXT DEFAULT NULL",
        "PreNormalize_PostalZipCode TEXT DEFAULT NULL",
        "PreNormalize_Country TEXT DEFAULT NULL",
        "PreNormalize_Website TEXT DEFAULT NULL",
        "PreNormalize_EmailOfCompany TEXT DEFAULT NULL",
        "PreNormalize_ContactName_First TEXT DEFAULT NULL",
        "PreNormalize_ContactName_Last TEXT DEFAULT NULL",
        "PreNormalize_Contact_TitlePosition TEXT DEFAULT NULL",
        "PreNormalize_EmailOfContact TEXT DEFAULT NULL",
        "PreNormalize_NumberPhone TEXT DEFAULT NULL",
        "PreNormalize_NumberFax TEXT DEFAULT NULL",
        "PreNormalize_LinkedInProfile TEXT DEFAULT NULL"
    ]
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## ADD MAILGUN EMAIL STATUS + PRENORMALIZE BACKUP COLUMNS TO DB TABLES
    print(f"\nADDING EMAIL STATUS + PRENORMALIZE BACKUP COLUMNS TO DB TABLES")

    ## LOOP THROUGH EACH TABLE
    for TableName in TableNames:

        ## LOOP THROUGH EACH COLUMN
        for Column in Columns:
        
            try:
        
                ## ADD COLUMN TO TABLE
                Query = f"ALTER TABLE {TableName} ADD COLUMN {Column}"
                Cursor.execute(Query)
                print(f"Added column to {TableName}: {Column.split()[0]}")
        
            except sqlite3.OperationalError as E:
        
                ## COLUMN ALREADY EXISTS
                if "duplicate column name" in str(E).lower():
                    print(f"Column already exists in {TableName}: {Column.split()[0]}")
                else:
                    ## OTHER ERROR
                    print(f"Error adding column to {TableName}: {E}")
    
    ## COMMIT CHANGES AND CLOSE CONNECTION
    Connection.commit()
    Connection.close()
    
    ## RETURN SUCCESS MESSAGE
    return "Email tracking columns and PreNormalize Columns added to all tables successfully."