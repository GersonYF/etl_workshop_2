from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError

credentials_directory = '/opt/airflow/dags/tasks/credentials.json'

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


def upload_file(file_path, folder_id):
    credentials = login()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink", "id": folder_id}]})
    file['title'] = file_path.split("/")[-1]
    file.SetContentFile(file_path)
    file.Upload()


def download_file_by_id(drive_id, download_path):
    credentials = login()
    file = credentials.CreateFile({'id': drive_id}) 
    file_name = file['title']
    file.GetContentFile(download_path + file_name)
