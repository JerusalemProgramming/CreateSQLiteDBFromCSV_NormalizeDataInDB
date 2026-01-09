## IMPORT MODULES
import csv
import os

## BEGIN DEFINE FUNCTION
def fn_WriteCSVFileFromCSV(OfficeData, OutputFilePath):
    
    ## WRITE OFFICE DATA DICTIONARY TO CSV FILE
    
    ## CREATE OUTPUT DIRECTORY IF NOT EXISTS
    OutputDir = os.path.dirname(OutputFilePath)
    if OutputDir and not os.path.exists(OutputDir):
        os.makedirs(OutputDir)
    
    ## COLLECT ALL CONTACTS FROM ALL OFFICES
    AllContacts = []
    for OfficeNumber, Contacts in OfficeData.items():
        AllContacts.extend(Contacts)
    
    ## RETURN IF NO CONTACTS
    if not AllContacts:
        return
    
    ## GET HEADERS FROM FIRST CONTACT TO PRESERVE ORDER
    Headers = list(AllContacts[0].keys())
    
    ## WRITE TO CSV FILE WITH SEMICOLON DELIMITER
    with open(OutputFilePath, 'w', encoding='utf-8', newline='') as CsvFile:
        Writer = csv.DictWriter(CsvFile, fieldnames=Headers, delimiter=';')
        Writer.writeheader()
        Writer.writerows(AllContacts)

## END DEFINE FUNCTION