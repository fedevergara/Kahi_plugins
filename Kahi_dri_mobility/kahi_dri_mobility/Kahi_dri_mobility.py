from google_auth_oauthlib.flow import InstalledAppFlow
from kahi_impactu_utils.Utils import check_date_format
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime as dt
from pickle import load, dump
from pandas import DataFrame
from re import match
import os.path

from kahi.KahiBase import KahiBase
from pymongo import MongoClient


class Kahi_dri_mobility(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        # self.collection.create_index("external_ids.id")

        self.int_mobility_file = self.config["dri_mobility"]["inter_mobility"]["file_id"]
        self.nac_mobility_file = self.config["dri_mobility"]["nat_mobility"]["file_id"]
        
        self.int_incoming_sheet = self.config["dri_mobility"]["in_sheet_name"]
        self.int_outgoing_sheet = self.config["dri_mobility"]["out_sheet_name"]

        self.scopes_url = self.config["dri_mobility"]["scopes_url"]
        self.pickle_file_path = self.config["dri_mobility"]["pickle_file_path"]
        self.credentials_file_path = self.config["dri_mobility"]["credentials_file_path"]

        self.already_in_db = []


    def gsheet_api_check(self, scopes, pickle_path, credentials_path):
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = load(token)
        else:
            if os.path.exists(pickle_path):
                with open(pickle_path, 'rb') as token:
                    creds = load(token)
                
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_path, scopes)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                dump(creds, token)
        return creds


    def pull_sheet_data(self, scopes, int_mobility_file, data_sheet, pickle_path, credentials_path):
        creds = self.gsheet_api_check(scopes, pickle_path, credentials_path)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=int_mobility_file,
            range=data_sheet).execute()
        values = result.get('values', [])
        
        if not values:
            print('No data found.')
        else:
            rows = sheet.values().get(spreadsheetId=int_mobility_file,
                                    range=data_sheet).execute()
            data = rows.get('values')
            return data


    def process_mobility(self):
        pass


    def run(self):
        scopes = self.scopes_url
        pickle_path = self.pickle_file_path
        credentials_path = self.credentials_file_path
        int_mobility_file = self.int_mobility_file

        incoming_sheet = self.int_incoming_sheet
        outgoing_sheet = self.int_outgoing_sheet

        incoming_data = self.pull_sheet_data(scopes, int_mobility_file, incoming_sheet, pickle_path, credentials_path)
        self.incoming = DataFrame(incoming_data[1:], columns=incoming_data[0])

        outgoing_data = self.pull_sheet_data(scopes, int_mobility_file ,outgoing_sheet, pickle_path, credentials_path)
        self.outgoing = DataFrame(outgoing_data[1:], columns=outgoing_data[0])

        self.process_mobility()
        return 0