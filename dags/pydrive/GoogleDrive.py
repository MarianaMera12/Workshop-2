import pandas as pd
import json
import logging
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import *

credentials_directory = './pydrive/credentials_module.json'


def login():
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_directory
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_directory)
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_directory)
    credentials = GoogleDrive(gauth)
    return credentials



def upload_file(file_path,id_folder):
    credentials = login()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                    "id": id_folder}]})
    file['title'] = file_path.split("/")[-1]
    file.SetContentFile(file_path)
    file.Upload()



def store(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)
    num_rows, num_cols = data.shape
    logging.info(f"DataFrame contains {num_rows} rows and {num_cols} columns.")

    data.to_csv('./data/merge_result.csv')

    upload_file("./data/merge_result.csv", "id_your_folder")
    logging.info("File 'merge_result.csv' stored and uploaded to Google Drive.")




