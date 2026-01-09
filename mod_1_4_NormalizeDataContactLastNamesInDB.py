## IMPORT MODULES
import sqlite3
import re

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataContactLastNamesInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR SPLITTING FULL NAMES IN FUNDOFFUND TABLE.
    SPLITS ContactName_Last (FULL NAME) INTO ContactName_First AND ContactName_Last.
    RETURNS STATUS REPORT STRING.
    """
    
    ## TABLE NAME
    TableName = "FundOfFund"
    
    ## BUILD REPORT
    Report = "CONTACT LAST NAME SPLITTING (FUNDOFFUND):\n"
    
    ## CONNECT TO DATABASE
    Conn = sqlite3.connect(DbPath)
    Cursor = Conn.cursor()
    
    try:
        ## PROCESS COUNTERS
        Processed = 0
        Normalized = 0
        
        ## GET ALL ROWS WITH FULL NAME IN ContactName_Last
        Cursor.execute(f"SELECT rowid, ContactName_Last FROM {TableName} WHERE ContactName_Last IS NOT NULL AND ContactName_Last != ''")
        Rows = Cursor.fetchall()
        
        for Rowid, FullName in Rows:
            if not FullName or FullName.strip() == "":
                continue
            
            Processed += 1
            
            ## NORMALIZE WHITESPACE
            Name = FullName.strip()
            Name = re.sub(r'\s+', ' ', Name)
            
            ## EXTRACT DR PREFIX
            DrPrefix = ""
            if Name.lower().startswith("dr ") or Name.lower().startswith("dr."):
                if Name.lower().startswith("dr."):
                    DrPrefix = Name[:3] + " "
                    Name = Name[3:].strip()
                else:
                    DrPrefix = Name[:3]
                    Name = Name[3:].strip()
            
            ## EXTRACT SUFFIX (SR, JR, III, IV, II)
            Suffix = ""
            SuffixPattern = r'\s+(Sr\.?|Jr\.?|III|IV|II)$'
            Match = re.search(SuffixPattern, Name, re.IGNORECASE)
            if Match:
                Suffix = " " + Match.group(1)
                Name = Name[:Match.start()].strip()
            
            ## CHECK FOR QUOTED NICKNAME
            QuotePattern = r'"[^"]+"'
            HasQuotedNickname = bool(re.search(QuotePattern, Name))
            
            ## SPLIT INTO WORDS
            Words = Name.split()
            
            ## HANDLE SINGLE WORD
            if len(Words) == 1:
                FirstName = DrPrefix + Words[0]
                LastName = Suffix.strip() if Suffix else Words[0]
            
            ## HANDLE TWO WORDS
            elif len(Words) == 2:
                FirstName = DrPrefix + Words[0]
                LastName = Words[1] + Suffix
            
            ## HANDLE QUOTED NICKNAME CASE
            elif HasQuotedNickname:
                ## LAST WORD IS LAST NAME, EVERYTHING ELSE IS FIRST
                LastName = Words[-1] + Suffix
                FirstName = DrPrefix + " ".join(Words[:-1])
            
            ## HANDLE THREE WORDS
            elif len(Words) == 3:
                ## NORMAL: FIRST + MIDDLE + LAST
                FirstName = DrPrefix + Words[0] + " " + Words[1]
                LastName = Words[2] + Suffix
            
            ## HANDLE FOUR+ WORDS (INITIAL + MULTIPLE MIDDLES + LAST)
            else:
                ## LAST WORD IS LAST NAME, REST IS FIRST/MIDDLE
                LastName = Words[-1] + Suffix
                FirstName = DrPrefix + " ".join(Words[:-1])
            
            ## GET CURRENT PRENORMALIZE VALUES
            Cursor.execute(f"SELECT PreNormalize_ContactName_First, PreNormalize_ContactName_Last FROM {TableName} WHERE rowid = ?", (Rowid,))
            PreNormFirst, PreNormLast = Cursor.fetchone()
            
            ## APPEND TO PRENORMALIZE IF ALREADY HAS DATA
            if PreNormLast and PreNormLast.strip():
                NewPreNormLast = PreNormLast + ", " + FullName
            else:
                NewPreNormLast = FullName
            
            if PreNormFirst and PreNormFirst.strip():
                NewPreNormFirst = PreNormFirst + ", "
            else:
                NewPreNormFirst = ""
            
            ## UPDATE PRENORMALIZE COLUMNS
            Cursor.execute(f"UPDATE {TableName} SET PreNormalize_ContactName_Last = ? WHERE rowid = ?", 
                         (NewPreNormLast, Rowid))
            Cursor.execute(f"UPDATE {TableName} SET PreNormalize_ContactName_First = ? WHERE rowid = ?", 
                         (NewPreNormFirst, Rowid))
            
            ## UPDATE NORMALIZED COLUMNS
            Cursor.execute(f"UPDATE {TableName} SET ContactName_First = ? WHERE rowid = ?", 
                         (FirstName, Rowid))
            Cursor.execute(f"UPDATE {TableName} SET ContactName_Last = ? WHERE rowid = ?", 
                         (LastName, Rowid))
            
            Normalized += 1
        
        ## COMMIT ALL CHANGES
        Conn.commit()
        
        Report += f"   - Processed {Processed} name fields\n"
        Report += f"   - Normalized {Normalized} name fields\n"
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
    print(fn_NormalizeDataContactLastNamesInDB("data.db"))