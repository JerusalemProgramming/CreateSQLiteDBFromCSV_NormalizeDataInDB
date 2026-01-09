## IMPORT MODULES
import csv

## BEGIN DEFINE FUNCTION
def fn_ReadCSVFile(FilePath):
    
    ## READ CSV FILE AND ASSIGN INCREMENTAL OFFICE NUMBERS
    
    ## DECLARE VARIABLES
    OfficeData = {}
    OfficeCounter = 1
    
    ## OPEN CSV FILE WITH SEMICOLON DELIMITER
    with open(FilePath, 'r', encoding='utf-8') as CsvFile:
        
        Reader = csv.DictReader(CsvFile, delimiter=';')
        
        for Row in Reader:
            
            ## ASSIGN INCREMENTAL OFFICE NUMBER AS STRING
            CurrentOfficeNumber = str(OfficeCounter)
            
            ## INITIALIZE LIST FOR NEW OFFICE NUMBER
            OfficeData[CurrentOfficeNumber] = []
            
            ## GET AND SPLIT PRIMARY CONTACT EMAILS
            EmailOfContactRaw = Row.get('EmailOfContact', '').strip()
            ContactEmails = []
            if EmailOfContactRaw:
                if ',' in EmailOfContactRaw:
                    ContactEmails = [Email.strip() for Email in EmailOfContactRaw.split(',') if Email.strip()]
                else:
                    ContactEmails = [EmailOfContactRaw]
            
            ## IF PRIMARY CONTACT EMAILS EXIST, CREATE SEPARATE ROW FOR EACH
            if ContactEmails:
                for ContactEmail in ContactEmails:
                    Contact = {
                        'OfficeNumber': CurrentOfficeNumber,
                        'FirmName': Row.get('FirmName', ''),
                        'ContactName_First': '',
                        'ContactName_Last': Row.get('Contact_Name', ''),
                        'Contact_TitlePosition': Row.get('Contact_TitlePosition', ''),
                        'NumberPhone': Row.get('NumberPhone', ''),
                        'EmailOfContact': ContactEmail,
                        'EmailOfCompany': Row.get('EmailOfCompany', ''),
                        'AddressOfCompany': Row.get('AddressOfCompany', ''),
                        'City': Row.get('City', ''),
                        'State_Province': Row.get('State_Province', ''),
                        'PostalZipCode': Row.get('PostalZipCode', ''),
                        'Country': Row.get('Country', ''),
                        'AUM_USD_MIL_UnlessNoted': Row.get('AUM_USD_MIL_UnlessNoted', ''),
                        'Website': Row.get('Website', ''),
                        'Notes': Row.get('Notes', '')
                    }
                    OfficeData[CurrentOfficeNumber].append(Contact)
            else:
                ## NO PRIMARY CONTACT EMAIL - CREATE BASE ROW AS-IS
                Contact = {
                    'OfficeNumber': CurrentOfficeNumber,
                    'FirmName': Row.get('FirmName', ''),
                    'ContactName_First': '',
                    'ContactName_Last': Row.get('Contact_Name', ''),
                    'Contact_TitlePosition': Row.get('Contact_TitlePosition', ''),
                    'NumberPhone': Row.get('NumberPhone', ''),
                    'EmailOfContact': '',
                    'EmailOfCompany': Row.get('EmailOfCompany', ''),
                    'AddressOfCompany': Row.get('AddressOfCompany', ''),
                    'City': Row.get('City', ''),
                    'State_Province': Row.get('State_Province', ''),
                    'PostalZipCode': Row.get('PostalZipCode', ''),
                    'Country': Row.get('Country', ''),
                    'AUM_USD_MIL_UnlessNoted': Row.get('AUM_USD_MIL_UnlessNoted', ''),
                    'Website': Row.get('Website', ''),
                    'Notes': Row.get('Notes', '')
                }
                OfficeData[CurrentOfficeNumber].append(Contact)
            
            ## ADD SECONDARY CONTACT IF EXISTS - WITH EMAIL SPLITTING
            if Row.get('SecondaryContact_Name', '').strip():
                
                ## GET AND SPLIT SECONDARY CONTACT EMAILS
                SecondaryEmailRaw = Row.get('SecondaryContact_EmailOfContact', '').strip()
                SecondaryEmails = []
                if SecondaryEmailRaw:
                    if ',' in SecondaryEmailRaw:
                        SecondaryEmails = [Email.strip() for Email in SecondaryEmailRaw.split(',') if Email.strip()]
                    else:
                        SecondaryEmails = [SecondaryEmailRaw]
                
                ## IF SECONDARY EMAILS EXIST, CREATE SEPARATE ROW FOR EACH
                if SecondaryEmails:
                    for SecondaryEmail in SecondaryEmails:
                        SecondaryContact = {
                            'OfficeNumber': CurrentOfficeNumber,
                            'FirmName': Row.get('FirmName', ''),
                            'ContactName_First': '',
                            'ContactName_Last': Row.get('SecondaryContact_Name', ''),
                            'Contact_TitlePosition': Row.get('SecondaryContact_TitlePosition', ''),
                            'NumberPhone': Row.get('SecondaryContact_NumberPhone', ''),
                            'EmailOfContact': SecondaryEmail,
                            'EmailOfCompany': '',
                            'AddressOfCompany': Row.get('AddressOfCompany', ''),
                            'City': Row.get('City', ''),
                            'State_Province': Row.get('State_Province', ''),
                            'PostalZipCode': Row.get('PostalZipCode', ''),
                            'Country': Row.get('Country', ''),
                            'AUM_USD_MIL_UnlessNoted': Row.get('AUM_USD_MIL_UnlessNoted', ''),
                            'Website': Row.get('Website', ''),
                            'Notes': Row.get('Notes', '')
                        }
                        OfficeData[CurrentOfficeNumber].append(SecondaryContact)
                else:
                    ## NO SECONDARY EMAIL - CREATE BASE SECONDARY ROW AS-IS
                    SecondaryContact = {
                        'OfficeNumber': CurrentOfficeNumber,
                        'FirmName': Row.get('FirmName', ''),
                        'ContactName_First': '',
                        'ContactName_Last': Row.get('SecondaryContact_Name', ''),
                        'Contact_TitlePosition': Row.get('SecondaryContact_TitlePosition', ''),
                        'NumberPhone': Row.get('SecondaryContact_NumberPhone', ''),
                        'EmailOfContact': '',
                        'EmailOfCompany': '',
                        'AddressOfCompany': Row.get('AddressOfCompany', ''),
                        'City': Row.get('City', ''),
                        'State_Province': Row.get('State_Province', ''),
                        'PostalZipCode': Row.get('PostalZipCode', ''),
                        'Country': Row.get('Country', ''),
                        'AUM_USD_MIL_UnlessNoted': Row.get('AUM_USD_MIL_UnlessNoted', ''),
                        'Website': Row.get('Website', ''),
                        'Notes': Row.get('Notes', '')
                    }
                    OfficeData[CurrentOfficeNumber].append(SecondaryContact)
            
            ## INCREMENT OFFICE COUNTER
            OfficeCounter += 1
    
    ## DEDUPLICATE AND ADD SPLIT COMPANY EMAILS PER OFFICE
    for OfficeNumber, ContactsList in OfficeData.items():
        CompanyEmailsAdded = set()
        if ContactsList:
            FirstContact = ContactsList[0]
            
            ## COLLECT ALL UNIQUE COMPANY EMAILS FROM ALL ROWS IN THIS OFFICE
            for Contact in ContactsList:
                EmailOfCompanyRaw = Contact.get('EmailOfCompany', '').strip()
                if EmailOfCompanyRaw:
                    if ',' in EmailOfCompanyRaw:
                        CompanyEmails = [Email.strip() for Email in EmailOfCompanyRaw.split(',') if Email.strip()]
                    else:
                        CompanyEmails = [EmailOfCompanyRaw]
                    
                    ## ADD EACH UNIQUE COMPANY EMAIL AS COMPANY-ONLY ROW
                    for CompanyEmail in CompanyEmails:
                        if CompanyEmail not in CompanyEmailsAdded:
                            CompanyEmailsAdded.add(CompanyEmail)
                            CompanyContact = {
                                'OfficeNumber': OfficeNumber,
                                'FirmName': FirstContact.get('FirmName', ''),
                                'ContactName_First': '',
                                'ContactName_Last': '',
                                'Contact_TitlePosition': '',
                                'NumberPhone': '',
                                'EmailOfContact': '',
                                'EmailOfCompany': CompanyEmail,
                                'AddressOfCompany': FirstContact.get('AddressOfCompany', ''),
                                'City': FirstContact.get('City', ''),
                                'State_Province': FirstContact.get('State_Province', ''),
                                'PostalZipCode': FirstContact.get('PostalZipCode', ''),
                                'Country': FirstContact.get('Country', ''),
                                'AUM_USD_MIL_UnlessNoted': FirstContact.get('AUM_USD_MIL_UnlessNoted', ''),
                                'Website': FirstContact.get('Website', ''),
                                'Notes': FirstContact.get('Notes', '')
                            }
                            ContactsList.append(CompanyContact)
    
    ## RETURN VARIABLES
    return(OfficeData)

## END DEFINE FUNCTION