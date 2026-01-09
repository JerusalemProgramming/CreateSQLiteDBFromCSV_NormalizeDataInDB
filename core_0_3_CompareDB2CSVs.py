## IMPORT MODULES
import sqlite3
import csv

## DECLARE VARIABLES
DBPath = "data.db"

## TABLE NAMES AND CORRESPONDING CSV FILES
TableCSVPairs = [
    ("FamilyOffices", "DATA_INPUT/DB_1_FamilyOffices.csv"),
    ("WealthManagement", "DATA_INPUT/DB_2_WealthManagement.csv"),
    ("Endowments", "DATA_INPUT/DB_3_Endowments.csv"),
    ("InstitutionalInvestment", "DATA_INPUT/DB_4_InstitutionalInvestment.csv"),
    ("InvestmentBanking", "DATA_INPUT/DB_5_InvestmentBanking.csv"),
    ("PrivateBanks", "DATA_INPUT/DB_6_PrivateBanks.csv"),
    ("MerchantBanks", "DATA_INPUT/DB_7_MerchantBanks.csv"),
    ("PensionFunds", "DATA_INPUT/DB_8_PensionFunds.csv"),
    ("FundOfFund", "DATA_INPUT/DB_9_FundOfFund.csv")
]

## BEGIN DEFINE FUNCTION
def fn_CompareDB2CSVs():

    ## VERIFY ALL DATABASE TABLES AGAINST SOURCE CSV FILES
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## BUILD VERIFICATION REPORT
    Report = f"\nDATABASE TO CSVs COMPARISON REPORT\n"
    Report += f"=" * 80 + "\n\n"
    
    TotalMismatches = 0
    
    ## LOOP THROUGH EACH TABLE-CSV PAIR
    for TableName, CSVFilePath in TableCSVPairs:
        
        ## GET DB ROW COUNT
        Cursor.execute(f"SELECT COUNT(*) FROM {TableName}")
        DBRowCount = Cursor.fetchone()[0]
        
        ## GET DB COLUMN COUNT
        Cursor.execute(f"PRAGMA table_info({TableName})")
        Columns = Cursor.fetchall()
        DBColumnCount = len(Columns)
        ColumnNames = [Col[1] for Col in Columns]
        
        ## REPLICATE EXACT MODULE LOGIC TO COUNT EXPECTED ROWS
        CSVRowCount = 0
        SecondaryContactCount = 0
        SplitContactEmailCount = 0
        CurrentOfficeNumber = None
        OfficeCompanyEmails = {}
        
        ## SPECIAL HANDLING FOR FUNDOFFUND (INCREMENTAL OFFICE NUMBERING)
        if TableName == "FundOfFund":
            OfficeCounter = 1
        
        with open(CSVFilePath, 'r', encoding='utf-8') as CsvFile:
            Reader = csv.DictReader(CsvFile, delimiter=';')
            for Row in Reader:
                CSVRowCount += 1
                
                ## HANDLE OFFICE NUMBER TRACKING
                if TableName == "FundOfFund":
                    ## FUNDOFFUND USES INCREMENTAL OFFICE NUMBERS
                    CurrentOfficeNumber = str(OfficeCounter)
                    OfficeCounter += 1
                else:
                    ## OTHER TABLES USE OFFICENUMBER FROM CSV
                    OfficeNum = Row.get('OfficeNumber', '').strip()
                    if OfficeNum:
                        CurrentOfficeNumber = OfficeNum
                
                ## INITIALIZE OFFICE IN COMPANY EMAIL TRACKER
                if CurrentOfficeNumber and CurrentOfficeNumber not in OfficeCompanyEmails:
                    OfficeCompanyEmails[CurrentOfficeNumber] = set()
                
                ## COUNT SPLIT CONTACT EMAILS (ADDITIONAL ROWS BEYOND BASE ROW)
                EmailOfContact = Row.get('EmailOfContact', '').strip()
                if EmailOfContact and ',' in EmailOfContact:
                    Emails = [E.strip() for E in EmailOfContact.split(',') if E.strip()]
                    if len(Emails) > 1:
                        SplitContactEmailCount += (len(Emails) - 1)
                
                ## COLLECT COMPANY EMAILS PER OFFICE FOR DEDUPLICATION
                EmailOfCompany = Row.get('EmailOfCompany', '').strip()
                if EmailOfCompany and CurrentOfficeNumber:
                    if ',' in EmailOfCompany:
                        Emails = [E.strip() for E in EmailOfCompany.split(',') if E.strip()]
                        for Email in Emails:
                            OfficeCompanyEmails[CurrentOfficeNumber].add(Email)
                    else:
                        OfficeCompanyEmails[CurrentOfficeNumber].add(EmailOfCompany)
                
                ## COUNT SECONDARY CONTACTS FOR FUNDOFFUND
                if TableName == "FundOfFund":
                    SecondaryName = Row.get('SecondaryContact_Name', '').strip()
                    if SecondaryName:
                        SecondaryContactCount += 1
                        
                        ## COUNT SPLIT SECONDARY CONTACT EMAILS
                        SecondaryEmail = Row.get('SecondaryContact_EmailOfContact', '').strip()
                        if SecondaryEmail and ',' in SecondaryEmail:
                            Emails = [E.strip() for E in SecondaryEmail.split(',') if E.strip()]
                            if len(Emails) > 1:
                                SplitContactEmailCount += (len(Emails) - 1)
        
        ## COUNT TOTAL DEDUPLICATED COMPANY EMAILS ACROSS ALL OFFICES
        TotalCompanyOnlyRows = sum(len(Emails) for Emails in OfficeCompanyEmails.values())
        
        ## CALCULATE EXPECTED DB ROWS
        if TableName == "FundOfFund":
            ExpectedDBRows = CSVRowCount + SecondaryContactCount + SplitContactEmailCount + TotalCompanyOnlyRows
        else:
            ExpectedDBRows = CSVRowCount + SplitContactEmailCount + TotalCompanyOnlyRows
        
        ## CHECK FOR MATCH
        IsMatch = (DBRowCount == ExpectedDBRows)
        
        if not IsMatch:
            TotalMismatches += 1
        
        ## BUILD STATUS MESSAGE
        if IsMatch:
            StatusMsg = "MATCH ✓"
        else:
            StatusMsg = f"MISMATCH ✗ (Expected: {ExpectedDBRows}, Got: {DBRowCount})"
        
        ## ADD TO REPORT
        Report += f"Table: {TableName}\n"
        Report += f"  CSV File: {CSVFilePath}\n"
        Report += f"  CSV Rows: {CSVRowCount}\n"
        
        if TableName == "FundOfFund" and SecondaryContactCount > 0:
            Report += f"  Secondary Contacts: {SecondaryContactCount}\n"
        
        if SplitContactEmailCount > 0:
            Report += f"  Split Contact Emails (additional rows): {SplitContactEmailCount}\n"
        
        if TotalCompanyOnlyRows > 0:
            Report += f"  Company-Only Rows (deduplicated per office): {TotalCompanyOnlyRows}\n"
        
        Report += f"  DB Rows: {DBRowCount}\n"
        Report += f"  DB Columns: {DBColumnCount}\n"
        Report += f"  Status: {StatusMsg}\n"
        Report += f"  Column Names: {', '.join(ColumnNames[:5])}{'...' if len(ColumnNames) > 5 else ''}\n\n"
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## ADD SUMMARY
    Report += f"=" * 80 + "\n"
    Report += f"SUMMARY: {len(TableCSVPairs) - TotalMismatches}/{len(TableCSVPairs)} tables verified\n"
    
    if TotalMismatches > 0:
        Report += f"WARNING: {TotalMismatches} table(s) have mismatches!\n"
    else:
        Report += f"STATUS: All tables verified successfully ✓\n"
    
    Report += f"=" * 80 + "\n"
    
    ## RETURN REPORT
    return Report

## END DEFINE FUNCTION