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
def fn_DiagnoseDB2CSVDiscrepancies():

    ## DIAGNOSE ROW COUNT DISCREPANCIES BETWEEN CSV AND DB
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## BUILD DIAGNOSTIC REPORT
    Report = f"\nDATABASE TO CSV DISCREPANCY DIAGNOSTIC REPORT\n"
    Report += f"=" * 80 + "\n\n"
    
    TotalDiscrepancies = 0
    
    ## LOOP THROUGH EACH TABLE-CSV PAIR
    for TableName, CSVFilePath in TableCSVPairs:
        
        Report += f"TABLE: {TableName}\n"
        Report += f"CSV FILE: {CSVFilePath}\n"
        Report += "-" * 80 + "\n"
        
        ## GET DB METRICS
        Cursor.execute(f"SELECT COUNT(*) FROM {TableName}")
        DBTotalRows = Cursor.fetchone()[0]
        
        ## COUNT DB COMPANY-ONLY ROWS (EMPTY EMAILOFCONTACT BUT HAS EMAILOFCOMPANY)
        Cursor.execute(f"""
            SELECT COUNT(*) FROM {TableName}
            WHERE (EmailOfContact IS NULL OR EmailOfContact = '')
            AND EmailOfCompany IS NOT NULL
            AND EmailOfCompany != ''
        """)
        DBCompanyOnlyRows = Cursor.fetchone()[0]
        
        ## COUNT DB CONTACT ROWS (HAS EMAILOFCONTACT)
        Cursor.execute(f"""
            SELECT COUNT(*) FROM {TableName}
            WHERE EmailOfContact IS NOT NULL
            AND EmailOfContact != ''
        """)
        DBContactRows = Cursor.fetchone()[0]
        
        ## REPLICATE EXACT MODULE LOGIC TO COUNT EXPECTED ROWS
        CSVTotalRows = 0
        CSVSecondaryContacts = 0
        SplitContactEmailCount = 0
        CurrentOfficeNumber = None
        OfficeCompanyEmails = {}
        
        ## SPECIAL HANDLING FOR FUNDOFFUND (INCREMENTAL OFFICE NUMBERING)
        if TableName == "FundOfFund":
            OfficeCounter = 1
        
        with open(CSVFilePath, 'r', encoding='utf-8') as CsvFile:
            Reader = csv.DictReader(CsvFile, delimiter=';')
            for Row in Reader:
                CSVTotalRows += 1
                
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
                        CSVSecondaryContacts += 1
                        
                        ## COUNT SPLIT SECONDARY CONTACT EMAILS
                        SecondaryEmail = Row.get('SecondaryContact_EmailOfContact', '').strip()
                        if SecondaryEmail and ',' in SecondaryEmail:
                            Emails = [E.strip() for E in SecondaryEmail.split(',') if E.strip()]
                            if len(Emails) > 1:
                                SplitContactEmailCount += (len(Emails) - 1)
        
        ## COUNT TOTAL DEDUPLICATED COMPANY EMAILS ACROSS ALL OFFICES
        TotalCompanyOnlyRows = sum(len(Emails) for Emails in OfficeCompanyEmails.values())
        
        ## CALCULATE EXPECTED VALUES
        ExpectedDBTotalRows = CSVTotalRows + CSVSecondaryContacts + SplitContactEmailCount + TotalCompanyOnlyRows
        
        ## CALCULATE DISCREPANCIES
        TotalRowDiscrepancy = DBTotalRows - ExpectedDBTotalRows
        
        ## BUILD REPORT SECTION
        Report += f"CSV ANALYSIS:\n"
        Report += f"  Total CSV rows: {CSVTotalRows}\n"
        
        if TableName == "FundOfFund" and CSVSecondaryContacts > 0:
            Report += f"  CSV secondary contacts: {CSVSecondaryContacts}\n"
        
        if SplitContactEmailCount > 0:
            Report += f"  Split contact emails (additional rows): {SplitContactEmailCount}\n"
        
        if TotalCompanyOnlyRows > 0:
            Report += f"  Company-only rows (deduplicated per office): {TotalCompanyOnlyRows}\n"
        
        Report += f"\nDB ANALYSIS:\n"
        Report += f"  Total DB rows: {DBTotalRows}\n"
        Report += f"  DB contact rows: {DBContactRows}\n"
        Report += f"  DB company-only rows: {DBCompanyOnlyRows}\n"
        
        Report += f"\nEXPECTED VALUES:\n"
        Report += f"  Expected DB total rows: {ExpectedDBTotalRows}\n"
        
        Report += f"\nDISCREPANCY ANALYSIS:\n"
        Report += f"  Total row discrepancy: {TotalRowDiscrepancy:+d}\n"
        
        ## CHECK FOR DISCREPANCIES
        if TotalRowDiscrepancy != 0:
            Report += f"\nSTATUS: DISCREPANCY DETECTED ✗\n"
            TotalDiscrepancies += 1
            
            if TotalRowDiscrepancy < 0:
                Report += f"  → DB has {abs(TotalRowDiscrepancy)} FEWER rows than expected\n"
            else:
                Report += f"  → DB has {TotalRowDiscrepancy} MORE rows than expected\n"
        else:
            Report += f"\nSTATUS: NO DISCREPANCY ✓\n"
        
        Report += "=" * 80 + "\n\n"
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## ADD SUMMARY
    Report += "=" * 80 + "\n"
    Report += "DIAGNOSTIC SUMMARY:\n"
    Report += f"  Tables with discrepancies: {TotalDiscrepancies}/{len(TableCSVPairs)}\n"
    Report += f"  Tables without discrepancies: {len(TableCSVPairs) - TotalDiscrepancies}/{len(TableCSVPairs)}\n"
    
    if TotalDiscrepancies == 0:
        Report += "\nOVERALL STATUS: All tables match expected values ✓\n"
    else:
        Report += f"\nOVERALL STATUS: {TotalDiscrepancies} table(s) have discrepancies ✗\n"
    
    Report += "=" * 80 + "\n"
    
    ## RETURN REPORT
    return Report

## END DEFINE FUNCTION