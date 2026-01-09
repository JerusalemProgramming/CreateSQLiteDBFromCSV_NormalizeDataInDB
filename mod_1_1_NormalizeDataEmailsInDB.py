## IMPORT MODULES
import sqlite3
import re

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataEmailsInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR EMAIL NORMALIZATION.
    PROCESSES ALL 9 TABLES AND NORMALIZES EMAIL DATA.
    RETURNS STATUS REPORT STRING.
    """
    
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
    
    ## EMAIL COLUMN NAMES
    EmailColumns = ["EmailOfContact", "EmailOfCompany"]
    
    ## BUILD REPORT
    Report = "EMAIL NORMALIZATION:\n"
    
    ## CONNECT TO DATABASE
    Conn = sqlite3.connect(DbPath)
    Cursor = Conn.cursor()
    
    try:
        ## PROCESS EACH TABLE
        TotalProcessed = 0
        TotalNormalized = 0
        
        for Table in TableNames:
            for EmailCol in EmailColumns:
                BackupCol = f"PreNormalize_{EmailCol}"
                
                ## GET ALL ROWS WITH EMAIL DATA
                Cursor.execute(f"SELECT rowid, {EmailCol} FROM {Table} WHERE {EmailCol} IS NOT NULL AND {EmailCol} != ''")
                Rows = Cursor.fetchall()
                
                for Rowid, OriginalEmail in Rows:
                    if not OriginalEmail or OriginalEmail.strip() == "":
                        continue
                    
                    TotalProcessed += 1
                    
                    ## NORMALIZE THE EMAIL
                    EmailText = OriginalEmail
                    
                    ## STEP 1: STRIP LEADING/TRAILING WHITESPACE
                    EmailText = EmailText.strip()
                    
                    ## STEP 2: REMOVE NEWLINES AND CARRIAGE RETURNS
                    EmailText = EmailText.replace('\n', '').replace('\r', '')
                    
                    ## STEP 3: REMOVE SPACES AROUND @ SYMBOLS
                    EmailText = re.sub(r'\s*@\s*', '@', EmailText)
                    
                    ## STEP 4: COLLAPSE EXCESSIVE WHITESPACE
                    EmailText = re.sub(r'\s+', ' ', EmailText)
                    
                    ## STEP 5: REPLACE SEPARATORS WITH COMMAS
                    EmailText = EmailText.replace(';', ',')
                    EmailText = re.sub(r'\s+or\s+', ',', EmailText, flags=re.IGNORECASE)
                    EmailText = re.sub(r'\s+and\s+', ',', EmailText, flags=re.IGNORECASE)
                    
                    ## STEP 6: EXTRACT ALL VALID EMAILS FROM TEXT
                    EmailPattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
                    ExtractedEmails = re.findall(EmailPattern, EmailText)
                    
                    ## STEP 7: IF NO VALID EMAILS FOUND, SET TO EMPTY STRING
                    if not ExtractedEmails:
                        NormalizedEmail = ""
                    else:
                        ## STEP 8: NORMALIZE EACH EMAIL (LOWERCASE, STRIP SPACES, REMOVE DUPLICATES)
                        NormalizedEmails = []
                        for Email in ExtractedEmails:
                            Email = Email.strip().lower()
                            if Email and Email not in NormalizedEmails:
                                NormalizedEmails.append(Email)
                        
                        ## STEP 9: JOIN MULTIPLE EMAILS WITH COMMA-SPACE
                        NormalizedEmail = ", ".join(NormalizedEmails)
                    
                    ## IF CHANGED, UPDATE BOTH BACKUP AND NORMALIZED COLUMNS
                    if NormalizedEmail != OriginalEmail:
                        TotalNormalized += 1
                        
                        ## COPY ORIGINAL TO BACKUP COLUMN
                        Cursor.execute(f"UPDATE {Table} SET {BackupCol} = ? WHERE rowid = ?", 
                                     (OriginalEmail, Rowid))
                        
                        ## UPDATE WITH NORMALIZED EMAIL
                        Cursor.execute(f"UPDATE {Table} SET {EmailCol} = ? WHERE rowid = ?", 
                                     (NormalizedEmail, Rowid))
        
        ## COMMIT ALL CHANGES
        Conn.commit()
        
        Report += f"   - Processed {TotalProcessed} email fields\n"
        Report += f"   - Normalized {TotalNormalized} email fields\n"
        Report += f"   - Status: SUCCESS"
        
    except Exception as E:
        Conn.rollback()
        Report += f"   - Status: FAILED - {str(E)}"
    
    finally:
        Conn.close()
    
    return Report

## TESTING
if __name__ == "__main__":
    
    # TEST THE NORMALIZATION
    print(fn_NormalizeDataEmailsInDB("data.db"))