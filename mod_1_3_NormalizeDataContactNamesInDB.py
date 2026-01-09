## IMPORT MODULES
import sqlite3
import re

## BEGIN DEFINE FUNCTION
def fn_NormalizeDataContactNamesInDB(DbPath="data.db"):
    """
    DATABASE OPERATIONS FOR CONTACT NAME NORMALIZATION.
    PROCESSES ALL 9 TABLES AND NORMALIZES FIRST/LAST NAME DATA.
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
    
    ## NAME COLUMN NAMES
    NameColumns = ["ContactName_First", "ContactName_Last"]
    
    ## LOWERCASE PARTICLES (NOBILITY/GEOGRAPHIC PREFIXES)
    LowercaseParticles = ["de", "van", "von", "der", "den", "del", "della", "di", "la", "le"]
    
    ## BUILD REPORT
    Report = "CONTACT NAME NORMALIZATION:\n"
    
    ## CONNECT TO DATABASE
    Conn = sqlite3.connect(DbPath)
    Cursor = Conn.cursor()
    
    try:
        ## PROCESS EACH TABLE
        FirstNameProcessed = 0
        FirstNameNormalized = 0
        LastNameProcessed = 0
        LastNameNormalized = 0
        
        for Table in TableNames:
            ## GET TABLE COLUMNS
            Cursor.execute(f"PRAGMA table_info({Table})")
            ExistingColumns = [Col[1] for Col in Cursor.fetchall()]
            
            for NameCol in NameColumns:
                ## CHECK IF COLUMN EXISTS IN THIS TABLE
                if NameCol not in ExistingColumns:
                    continue
                
                BackupCol = f"PreNormalize_{NameCol}"
                
                ## GET ALL ROWS WITH NAME DATA
                Cursor.execute(f"SELECT rowid, {NameCol} FROM {Table} WHERE {NameCol} IS NOT NULL AND {NameCol} != ''")
                Rows = Cursor.fetchall()
                
                for Rowid, OriginalName in Rows:
                    if not OriginalName or OriginalName.strip() == "":
                        continue
                    
                    ## INCREMENT PROCESSED COUNT
                    if NameCol == "ContactName_First":
                        FirstNameProcessed += 1
                    else:
                        LastNameProcessed += 1
                    
                    ## NORMALIZE THE NAME
                    NameText = OriginalName
                    
                    ## STEP 1: STRIP LEADING/TRAILING WHITESPACE
                    NameText = NameText.strip()
                    
                    ## STEP 2: COLLAPSE MULTIPLE SPACES TO SINGLE SPACE
                    NameText = re.sub(r'\s+', ' ', NameText)
                    
                    ## STEP 3: ENSURE SINGLE SPACE AFTER PERIODS
                    NameText = re.sub(r'\.\s+', '. ', NameText)
                    
                    ## STEP 4: CHECK IF NAME IS ALL CAPS (NEEDS TITLE CASE CONVERSION)
                    IsAllCaps = NameText.isupper()
                    
                    if IsAllCaps:
                        ## CONVERT TO TITLE CASE ONLY IF ALL CAPS
                        NameText = NameText.title()
                        
                        ## FIX MC PREFIX (Mcfarlane -> McFarlane)
                        NameText = re.sub(r'\bMc([a-z])', lambda m: 'Mc' + m.group(1).upper(), NameText)
                        
                        ## FIX MAC PREFIX (Macdonald -> MacDonald)
                        NameText = re.sub(r'\bMac([a-z])', lambda m: 'Mac' + m.group(1).upper(), NameText)
                        
                        ## FIX O' PREFIX (O'Brien -> O'Brien, O'connor -> O'Connor)
                        NameText = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), NameText)
                    
                    ## STEP 5: HANDLE LOWERCASE PARTICLES (ONLY FOR LAST NAMES)
                    if NameCol == "ContactName_Last":
                        Words = NameText.split()
                        NormalizedWords = []
                        
                        for i, Word in enumerate(Words):
                            ## CHECK IF WORD (WITHOUT PUNCTUATION) IS A PARTICLE
                            CleanWord = Word.strip('.,;:!?')
                            ## COMPARE LOWERCASE VERSION TO PARTICLE LIST
                            if CleanWord.lower() in LowercaseParticles:
                                ## CONVERT PARTICLE TO LOWERCASE
                                if Word.endswith(tuple('.,;:!?')):
                                    ## PRESERVE PUNCTUATION
                                    Punctuation = Word[len(CleanWord):]
                                    NormalizedWords.append(CleanWord.lower() + Punctuation)
                                else:
                                    NormalizedWords.append(CleanWord.lower())
                            else:
                                ## KEEP AS IS
                                NormalizedWords.append(Word)
                        
                        NameText = ' '.join(NormalizedWords)
                    
                    ## IF CHANGED, UPDATE BOTH BACKUP AND NORMALIZED COLUMNS
                    if NameText != OriginalName:
                        ## INCREMENT NORMALIZED COUNT
                        if NameCol == "ContactName_First":
                            FirstNameNormalized += 1
                        else:
                            LastNameNormalized += 1
                        
                        ## COPY ORIGINAL TO BACKUP COLUMN
                        Cursor.execute(f"UPDATE {Table} SET {BackupCol} = ? WHERE rowid = ?", 
                                     (OriginalName, Rowid))
                        
                        ## UPDATE WITH NORMALIZED NAME
                        Cursor.execute(f"UPDATE {Table} SET {NameCol} = ? WHERE rowid = ?", 
                                     (NameText, Rowid))
        
        ## COMMIT ALL CHANGES
        Conn.commit()
        
        Report += f"   - Processed {FirstNameProcessed} first name fields\n"
        Report += f"   - Normalized {FirstNameNormalized} first name fields\n"
        Report += f"   - Processed {LastNameProcessed} last name fields\n"
        Report += f"   - Normalized {LastNameNormalized} last name fields\n"
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
    print(fn_NormalizeDataContactNamesInDB("data.db"))