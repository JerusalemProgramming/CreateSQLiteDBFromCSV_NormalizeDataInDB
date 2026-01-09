## IMPORT MODULES
import sqlite3
import re

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataPhoneFaxNumbersInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR PHONE/FAX NORMALIZATION.
    PROCESSES ALL 9 TABLES AND NORMALIZES PHONE/FAX DATA.
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
    
    ## PHONE/FAX COLUMN NAMES
    NumberColumns = ["NumberPhone", "NumberFax"]
    
    ## BUILD REPORT
    Report = "PHONE/FAX NORMALIZATION:\n"
    
    ## CONNECT TO DATABASE
    Conn = sqlite3.connect(DbPath)
    Cursor = Conn.cursor()
    
    try:
        ## PROCESS EACH TABLE
        PhoneProcessed = 0
        PhoneNormalized = 0
        FaxProcessed = 0
        FaxNormalized = 0
        
        for Table in TableNames:
            ## GET TABLE COLUMNS
            Cursor.execute(f"PRAGMA table_info({Table})")
            ExistingColumns = [Col[1] for Col in Cursor.fetchall()]
            
            for NumberCol in NumberColumns:
                ## CHECK IF COLUMN EXISTS IN THIS TABLE
                if NumberCol not in ExistingColumns:
                    continue
                
                BackupCol = f"PreNormalize_{NumberCol}"
                
                ## GET ALL ROWS WITH PHONE/FAX DATA
                Cursor.execute(f"SELECT rowid, {NumberCol} FROM {Table} WHERE {NumberCol} IS NOT NULL AND {NumberCol} != ''")
                Rows = Cursor.fetchall()
                
                for Rowid, OriginalNumber in Rows:
                    if not OriginalNumber or OriginalNumber.strip() == "":
                        continue
                    
                    ## INCREMENT PROCESSED COUNT
                    if NumberCol == "NumberPhone":
                        PhoneProcessed += 1
                    else:
                        FaxProcessed += 1
                    
                    ## NORMALIZE THE NUMBER
                    NumberText = OriginalNumber
                    
                    ## STEP 1: STRIP LEADING/TRAILING WHITESPACE
                    NumberText = NumberText.strip()
                    
                    ## STEP 2: COLLAPSE MULTIPLE SPACES TO SINGLE SPACE
                    NumberText = re.sub(r'\s+', ' ', NumberText)
                    
                    ## IF CHANGED, UPDATE BOTH BACKUP AND NORMALIZED COLUMNS
                    if NumberText != OriginalNumber:
                        ## INCREMENT NORMALIZED COUNT
                        if NumberCol == "NumberPhone":
                            PhoneNormalized += 1
                        else:
                            FaxNormalized += 1
                        
                        ## COPY ORIGINAL TO BACKUP COLUMN
                        Cursor.execute(f"UPDATE {Table} SET {BackupCol} = ? WHERE rowid = ?", 
                                     (OriginalNumber, Rowid))
                        
                        ## UPDATE WITH NORMALIZED NUMBER
                        Cursor.execute(f"UPDATE {Table} SET {NumberCol} = ? WHERE rowid = ?", 
                                     (NumberText, Rowid))
        
        ## COMMIT ALL CHANGES
        Conn.commit()
        
        Report += f"   - Processed {PhoneProcessed} phone fields\n"
        Report += f"   - Normalized {PhoneNormalized} phone fields\n"
        Report += f"   - Processed {FaxProcessed} fax fields\n"
        Report += f"   - Normalized {FaxNormalized} fax fields\n"
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
    print(fn_NormalizeDataPhoneFaxNumbersInDB("data.db"))