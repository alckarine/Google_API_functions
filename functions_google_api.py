from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, timedelta, datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import gspread
import pandas as pd
import time
from tqdm import tqdm
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
import requests
import httplib2
from googleapiclient.http import MediaFileUpload


# importar planilha do gsheets como dataframe
def get_google_sheets(ID, JSON, aba, line_header=0, line_values=1):
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = Credentials.from_service_account_file(JSON, scopes=scope)
    gc = gspread.authorize(credentials)
    spreadsheet_key = ID
    book = gc.open_by_key(spreadsheet_key)
    worksheet = book.worksheet(aba)
    table = worksheet.get_all_values()
    df = pd.DataFrame(table[line_values:], columns=table[line_header])


    return df


# Limpar planilha do google sheets e colar novo dataframe
def open_clean_sheets(JSON, ID, aba, df=None, r=2, c=1, firstClear_row=3, first_col='A', last_col='AW'):
    gc = gspread.service_account(JSON)
    sh = gc.open_by_key(ID)
    time.sleep(5)
    worksheet = sh.worksheet(aba)
    worksheet.batch_clear(["{}{}:{}".format(first_col, firstClear_row, last_col)])
    
    set_with_dataframe(worksheet, df, row=r, col=c, include_column_header=True)

# listar arquivos xlsx em uma pasta do Google drive
def get_xlsx_files(JSON_path, topFolderId):
    scopes = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(JSON_path, scopes=scopes)
    service = build('drive', 'v3', credentials=credentials, static_discovery=False)

    pageToken = ""
    while pageToken is not None:
        response = service.files().list(q="'" + topFolderId + "' in parents", pageSize=1000, pageToken=pageToken, fields="nextPageToken, files(id, name)").execute()
        pageToken = response.get('nextPageToken')
    resposta = response.get('files')

    return resposta

# pegar todos os arquivos xlsx de uma pasta no Google drive e juntar em um dataframe
def get_xlsx_df(service_account_name, JSON_path, lista_xlsx_files, lista_abas):
    SCOPES = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_path, SCOPES)
    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build(serviceName='drive', version='v3', http=http, static_discovery=False)

    df = pd.DataFrame()

    for file in tqdm(lista_xlsx_files):
        sheets_id = file['id']
        sheets_name = file['name']
        url = f"https://docs.google.com/spreadsheets/export?id={sheets_id}&exportFormat=xlsx"
        res = requests.get(url, headers={"Authorization": "Bearer " + credentials.access_token})
        xl = pd.ExcelFile(BytesIO(res.content))
        for sheet_name in xl.sheet_names:
            if sheet_name in lista_abas:
                df_temp = pd.read_excel(BytesIO(res.content), sheet_name=sheet_name)
                df_temp['origin_file'] = sheets_name
                df = pd.concat([df, df_temp])
    return df


# criar lista de todos os dias entre uma data inicial e uma data final
def listDaysRange(data_inicial, data_final):
    start = datetime.strptime(data_inicial, '%Y-%m-%d')
    end = datetime.strptime(data_final, '%Y-%m-%d')
    delta_days = (end-start).days
    list_days = [data_inicial]
    for i in range(delta_days-1):
        j = (start + timedelta(days=i+1)).strftime('%Y-%m-%d')
        list_days.append(j)
    list_days.append(data_final)

    list_days = list(set(list_days))
    return list_days


# adicionar 0 nos valores de uma coluna, caso sejam menores que 10
def adiciona_zero(x):
    if x < 10:
        x = '0' + str(x)
    else:
        x = str(x)
    return x


# fazer upload de arquivos csv para Google Drive
def upload_csv_drive(JSON_path, file_name_with_extension, folder_id):
    SERVICE_ACCOUNT_FILE = JSON_path
    SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/drive.metadata.readonly']
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials, static_discovery=False)
    print('service ok')
    mime_types = 'text/csv'
    name = file_name_with_extension
    print(name)
    folder_id = folder_id
    print(folder_id)
    file_metadata = {
        'name': name,
        'parents': [folder_id],
        'mimeType': mime_types
    }
    media = MediaFileUpload(name, mimetype=mime_types)
    print('media ok')
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print('upload csv ok')


# Apagar arquivos no Google drive
def delete_drive_files(list_files_ids, JSON_path):

    SCOPES = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_path, SCOPES)
    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build(serviceName='drive', version='v3', http=http, static_discovery=False)

    for file_id in list_files_ids:
        service.files().delete(fileId=file_id).execute()
