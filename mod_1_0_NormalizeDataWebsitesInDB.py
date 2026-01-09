## IMPORT MODULES
import sqlite3

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataWebsitesInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR WEBSITE NORMALIZATION.
    PROCESSES ALL TABLES AND NORMALIZES WEBSITE DATA.
    RETURNS STATUS REPORT STRING.
    """
    
    ## BUILD REPORT
    Report = "WEBSITE NORMALIZATION:\n"
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DbPath)
    Cursor = Connection.cursor()
    
    try:
        ## PROCESS COUNTERS
        TotalProcessed = 0
        TotalNormalized = 0
        
        ## GET ALL TABLE NAMES
        Cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        Tables = Cursor.fetchall()
        
        ## LOOP THROUGH EACH TABLE
        for TableTuple in Tables:
            TableName = TableTuple[0]
            
            ## CHECK IF TABLE HAS WEBSITE COLUMN
            Cursor.execute(f"PRAGMA table_info({TableName})")
            Columns = [col[1] for col in Cursor.fetchall()]
            
            if "Website" not in Columns:
                continue
            
            ## GET ALL ROWS WITH WEBSITE
            Cursor.execute(f"SELECT rowid, Website FROM {TableName} WHERE Website IS NOT NULL AND Website != ''")
            Rows = Cursor.fetchall()
            
            ## NORMALIZE EACH WEBSITE
            for RowID, Website in Rows:
                TotalProcessed += 1
                
                OriginalWebsite = Website
                NormalizedWebsite = Website.strip()
                
                ## REMOVE HTTP://
                if NormalizedWebsite.lower().startswith("http://"):
                    NormalizedWebsite = NormalizedWebsite[7:]
                
                ## REMOVE HTTPS://
                if NormalizedWebsite.lower().startswith("https://"):
                    NormalizedWebsite = NormalizedWebsite[8:]
                
                ## REMOVE WWW.
                if NormalizedWebsite.lower().startswith("www."):
                    NormalizedWebsite = NormalizedWebsite[4:]
                
                ## REMOVE FIRST TRAILING SLASH AND EVERYTHING AFTER
                SlashIndex = NormalizedWebsite.find("/")
                if SlashIndex != -1:
                    NormalizedWebsite = NormalizedWebsite[:SlashIndex]
                
                ## UPDATE IF CHANGED
                if NormalizedWebsite != OriginalWebsite:
                    TotalNormalized += 1
                    
                    ## GET CURRENT PRENORMALIZE VALUE
                    Cursor.execute(f"SELECT PreNormalize_Website FROM {TableName} WHERE rowid = ?", (RowID,))
                    PreNormValue = Cursor.fetchone()[0]
                    
                    ## APPEND TO PRENORMALIZE IF ALREADY HAS DATA
                    if PreNormValue and PreNormValue.strip():
                        NewPreNormValue = PreNormValue + ", " + OriginalWebsite
                    else:
                        NewPreNormValue = OriginalWebsite
                    
                    ## UPDATE PRENORMALIZE COLUMN
                    Cursor.execute(f"UPDATE {TableName} SET PreNormalize_Website = ? WHERE rowid = ?", 
                                 (NewPreNormValue, RowID))
                    
                    ## UPDATE NORMALIZED WEBSITE
                    Cursor.execute(f"UPDATE {TableName} SET Website = ? WHERE rowid = ?", (NormalizedWebsite, RowID))
        
        ## COMMIT ALL CHANGES
        Connection.commit()
        
        Report += f"   - Processed {TotalProcessed} website fields\n"
        Report += f"   - Normalized {TotalNormalized} website fields\n"
        Report += f"   - Status: SUCCESS"
        
    except Exception as E:
        Connection.rollback()
        Report += f"   - Status: FAILED - {str(E)}"
    
    finally:
        Connection.close()
    
    return Report
## END DEFINE FUNCTION

## TESTING
if __name__ == "__main__":
    
    # TEST THE NORMALIZATION
    print(fn_NormalizeDataWebsitesInDB("data.db"))