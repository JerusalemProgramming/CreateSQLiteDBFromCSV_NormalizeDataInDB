## IMPORT MODULES
import sqlite3
import re

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataMiscRemainingInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR MISCELLANEOUS REMAINING FIELD NORMALIZATION.
    PROCESSES ALL 9 TABLES AND NORMALIZES FIRMNAME, ADDRESS, CITY, STATE, POSTAL, AND TITLE DATA.
    PRESERVES NEWLINE BREAKS FOR ADDRESS FORMATTING.
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
    
    ## COLUMNS TO NORMALIZE
    NormalizeColumns = [
        "FirmName",
        "AddressOfCompany",
        "City",
        "State_Province",
        "PostalZipCode",
        "Contact_TitlePosition"
    ]
    
    ## BUILD REPORT
    Report = "MISCELLANEOUS FIELD NORMALIZATION:\n"
    
    ## CONNECT TO DATABASE
    Conn = sqlite3.connect(DbPath)
    Cursor = Conn.cursor()
    
    try:
        ## PROCESS COUNTERS
        TotalProcessed = 0
        TotalNormalized = 0
        
        ## PROCESS EACH TABLE
        for Table in TableNames:
            ## GET TABLE COLUMNS
            Cursor.execute(f"PRAGMA table_info({Table})")
            ExistingColumns = [Col[1] for Col in Cursor.fetchall()]
            
            for Column in NormalizeColumns:
                ## CHECK IF COLUMN EXISTS IN THIS TABLE
                if Column not in ExistingColumns:
                    continue
                
                BackupCol = f"PreNormalize_{Column}"
                
                ## GET ALL ROWS WITH DATA IN THIS COLUMN
                Cursor.execute(f"SELECT rowid, {Column} FROM {Table} WHERE {Column} IS NOT NULL AND {Column} != ''")
                Rows = Cursor.fetchall()
                
                for Rowid, OriginalValue in Rows:
                    if not OriginalValue or OriginalValue.strip() == "":
                        continue
                    
                    TotalProcessed += 1
                    
                    ## NORMALIZE THE VALUE
                    NormalizedValue = OriginalValue
                    
                    ## STEP 1: STRIP LEADING/TRAILING WHITESPACE FROM OVERALL TEXT
                    NormalizedValue = NormalizedValue.strip()
                    
                    ## STEP 2: SPLIT BY NEWLINES, NORMALIZE EACH LINE, REJOIN
                    Lines = NormalizedValue.split('\n')
                    NormalizedLines = []
                    for Line in Lines:
                        ## STRIP WHITESPACE FROM EACH LINE
                        Line = Line.strip()
                        ## COLLAPSE MULTIPLE SPACES TO SINGLE SPACE
                        Line = re.sub(r' +', ' ', Line)
                        NormalizedLines.append(Line)
                    
                    NormalizedValue = '\n'.join(NormalizedLines)
                    
                    ## IF CHANGED, UPDATE BOTH BACKUP AND NORMALIZED COLUMNS
                    if NormalizedValue != OriginalValue:
                        TotalNormalized += 1
                        
                        ## GET CURRENT PRENORMALIZE VALUE
                        Cursor.execute(f"SELECT {BackupCol} FROM {Table} WHERE rowid = ?", (Rowid,))
                        PreNormValue = Cursor.fetchone()[0]
                        
                        ## APPEND TO PRENORMALIZE IF ALREADY HAS DATA
                        if PreNormValue and PreNormValue.strip():
                            NewPreNormValue = PreNormValue + ", " + OriginalValue
                        else:
                            NewPreNormValue = OriginalValue
                        
                        ## UPDATE PRENORMALIZE COLUMN
                        Cursor.execute(f"UPDATE {Table} SET {BackupCol} = ? WHERE rowid = ?", 
                                     (NewPreNormValue, Rowid))
                        
                        ## UPDATE WITH NORMALIZED VALUE
                        Cursor.execute(f"UPDATE {Table} SET {Column} = ? WHERE rowid = ?", 
                                     (NormalizedValue, Rowid))
        
        ## COMMIT ALL CHANGES
        Conn.commit()
        
        Report += f"   - Processed {TotalProcessed} fields\n"
        Report += f"   - Normalized {TotalNormalized} fields\n"
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
    print(fn_NormalizeDataMiscRemainingInDB("data.db"))