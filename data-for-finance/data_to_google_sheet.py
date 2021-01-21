from __future__ import print_function
import pickle
import os.path
import pandas as pd

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from mysql_queries import MySQLQueries
from db_connect import GetData

# The code builds up on the Google API documentation for Google Sheets which can be found here:
# https://developers.google.com/sheets/api/quickstart/python

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# ------------------------------------------------------------------------------------------   
# Defining the sheet and ranges where data is pasted and where its deleted
# ------------------------------------------------------------------------------------------   

# The ID of the sheet
# FinanceDataSheet = '10E-zWpRGYyBB-Cg4Nm_55qbNG1kfVqFHSun3bXdTatM' -Sibel's test file
FinanceDataSheet = '1ytD9giP51Iwi1_2SXgjLVOJuehbRHjvSEh6QOwce7TM'

# Ranges for pasting/deleting data in the respective sheets
ContactsRange = 'ContactsData!A2'
ContactsDelete = 'ContactsData!A2:Z'

DealsRange = 'DealsData!A2'
DealsDelete = 'DealsData!A2:Z'

PipelineRange = 'PipelineData!A2'
PipelineDelete = 'PipelineData!A2:Z'

FunnelRange = 'CostFunnel!A2'
FunnelDelete = 'CostFunnel!A2:Z'

# ------------------------------------------------------------------------------------------   
# Google Authentification
# ------------------------------------------------------------------------------------------   

def main():

    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

# ------------------------------------------------------------------------------------------                             
# Clearing the Sheets
# ------------------------------------------------------------------------------------------   
    
    # Call the Sheets API
    sheet = service.spreadsheets()
    
    # Defining Empty body
    body = {}
                                               
    ContactsClear = service.spreadsheets().values().clear(
      spreadsheetId=FinanceDataSheet, range=ContactsDelete, body=body).execute()
    
    DealsClear = service.spreadsheets().values().clear(
      spreadsheetId=FinanceDataSheet, range=DealsDelete, body=body).execute()
      
    PipelineClear = service.spreadsheets().values().clear(
      spreadsheetId=FinanceDataSheet, range=PipelineDelete, body=body).execute()
      
    FunnelClear = service.spreadsheets().values().clear(
      spreadsheetId=FinanceDataSheet, range=FunnelDelete, body=body).execute()
                      
# ------------------------------------------------------------------------------------------                             
# Establishing all connections and getting data in the right "format"
# ------------------------------------------------------------------------------------------   
                          
    # Call the MySQL connection
    mysql_connection = GetData.connection_string()

    # Call the queries
    contacts_query = MySQLQueries().get_ContactsQuery()
    deals_query = MySQLQueries().get_DealsQuery()
    pipeline_query = MySQLQueries().get_PipelineQuery()
    funnel_query = MySQLQueries().get_FunnelQuery()

    # Creating the dataframe based on the queries
    df_contacts = pd.read_sql(contacts_query, con=mysql_connection)
    df_deals = pd.read_sql(deals_query, con=mysql_connection)
    df_pipeline = pd.read_sql(pipeline_query, con=mysql_connection)
    df_funnel = pd.read_sql(funnel_query, con=mysql_connection)

    # Creating the lists from the dataframes
    df_contacts = df_contacts.values.tolist()
    df_deals = df_deals.values.tolist()
    df_pipeline = df_pipeline.values.tolist()
    df_funnel = df_funnel.values.tolist()

    contacts_body = {'values': df_contacts}
    deals_body = {'values': df_deals}
    pipeline_body = {'values': df_pipeline}
    funnel_body = {'values': df_funnel}
                          
# ------------------------------------------------------------------------------------------   
# Writing Data to Google Sheets 
# ------------------------------------------------------------------------------------------   

    ContactsUpdate = service.spreadsheets().values().append(
        spreadsheetId=FinanceDataSheet, range=ContactsRange,
        valueInputOption='USER_ENTERED', body=contacts_body).execute()

    print('ContactsData: {0} cells appended.'.format(ContactsUpdate
                                       .get('updates')
                                       .get('updatedCells')))

    DealsUpdate = service.spreadsheets().values().append(
        spreadsheetId=FinanceDataSheet, range=DealsRange,
        valueInputOption='USER_ENTERED', body=deals_body).execute()

    print('DealsData: {0} cells appended.'.format(DealsUpdate
                                       .get('updates')
                                       .get('updatedCells')))

    PipelineUpdate = service.spreadsheets().values().append(
        spreadsheetId=FinanceDataSheet, range=PipelineRange,
        valueInputOption='USER_ENTERED', body=pipeline_body).execute()

    print('PipelineData: {0} cells appended.'.format(PipelineUpdate
                                       .get('updates')
                                       .get('updatedCells')))

    FunnelUpdate = service.spreadsheets().values().append(
        spreadsheetId=FinanceDataSheet, range=FunnelRange,
        valueInputOption='USER_ENTERED', body=funnel_body).execute()

    print('FunnelData: {0} cells appended.'.format(FunnelUpdate
                                       .get('updates')
                                       .get('updatedCells')))

if __name__ == '__main__':
    main()