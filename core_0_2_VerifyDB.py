## IMPORT MODULES
import sqlite3

## DECLARE VARIABLES
DBPath = "data.db"

## BEGIN DEFINE FUNCTION
def fn_VerifyDB():

    # VERIFY DATABASE INTEGRITY AND RETURN SUMMARY OF ALL TABLES
    
    ## CONNECT TO DATABASE
    Connection = sqlite3.connect(DBPath)
    Cursor = Connection.cursor()
    
    ## GET ALL TABLE NAMES
    Cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    Tables = Cursor.fetchall()
    
    ## BUILD VERIFICATION REPORT
    Report = f"\nDATABASE VERIFICATION REPORT\n"
    Report += f"=" * 50 + "\n"
    Report += f"Database: {DBPath}\n"
    Report += f"Total Tables: {len(Tables)}\n\n"
    
    ## LOOP THROUGH EACH TABLE
    for TableTuple in Tables:
        TableName = TableTuple[0]
        
        ## GET ROW COUNT
        Cursor.execute(f"SELECT COUNT(*) FROM {TableName}")
        RowCount = Cursor.fetchone()[0]
        
        ## GET COLUMN INFO
        Cursor.execute(f"PRAGMA table_info({TableName})")
        Columns = Cursor.fetchall()
        ColumnCount = len(Columns)
        
        ## ADD TO REPORT
        Report += f"Table: {TableName}\n"
        Report += f"  Rows: {RowCount}\n"
        Report += f"  Columns: {ColumnCount}\n"
        Report += f"  Column Names: {', '.join([col[1] for col in Columns])}\n\n"
    
    ## CLOSE CONNECTION
    Connection.close()
    
    ## RETURN REPORT
    return Report
## END DEFINE FUNCTION