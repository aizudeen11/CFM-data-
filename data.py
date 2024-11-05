from googleapiclient.discovery import build
from google.oauth2 import service_account
import zipfile, os, pandas as pd, requests
from datetime import datetime, date
from googleapiclient.http import MediaIoBaseDownload
import io

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'Data/service_account.json'
PARENT_FOLDER_ID = "1Bi5-If_g5MmDKCpMNSuVRRDUOLeiw3pQ"

def authenticate():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return creds

creds = authenticate()
service = build('drive', 'v3', credentials=creds)

def upload_file(file_path:list, file_names:list):    
    f = zip(file_path, file_names)
    for path, names in f:
        file_metadata = {
            'name' : names,
            'parents' : [PARENT_FOLDER_ID]
        }

        file = service.files().create(
            body=file_metadata,
            media_body=path
        ).execute()
        print(f'{names} uploded to gdrive')

def delete_files(file_or_folder_id):
    """Delete a file or folder in Google Drive by ID."""
    try:
        service.files().delete(fileId=file_or_folder_id).execute()
        print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
    except Exception as e:
        print(f"Error deleting file/folder with ID: {file_or_folder_id}")
        print(f"Error details: {str(e)}")

def list_folder(parent_folder_id=PARENT_FOLDER_ID, delete=False):
    """List folders and files in Google Drive."""
    results = service.files().list(
        q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    if not items:
        print("No folders or files found in Google Drive.")
    else:
        print("Folders and files in Google Drive:")
        for item in items:
            print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}")
            if delete:
                delete_files(item['id'])

def download_file(file_id, destination_path):
    """Download a file from Google Drive by its ID."""
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination_path, mode='wb')
    
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

class DataCompile:

    def __init__(self, file_dir:str='download zip', file_dir2:str='last download'):
        self.file_dir = file_dir
        self.file_dir2 = file_dir2

    def clear_dir(self):

        '''clear directory /download zip'''
        if not os.path.exists(self.file_dir):
            os.makedirs(self.file_dir)
        if os.listdir(self.file_dir):
            for x in os.listdir(self.file_dir):
                os.remove(self.file_dir + '/' + x)
        print('cleared download zip folder...')

    def clear_dir2(self):

        '''clear directory /last download'''
        if not os.path.exists(self.file_dir2):
            os.makedirs(self.file_dir2)
        if os.listdir(self.file_dir2):
            for x in os.listdir(self.file_dir2):
                os.remove(self.file_dir2 + '/' + x)
        print('cleared last download folder...')
        
    def bis_liquidity(self):

        '''BIS Liquidity data for past 10 years'''

        today = date.today()
        past_yr = str(int(str(today)[:4])-11) + str(today)[4:]

        urls = [f"https://stats.bis.org/api/v2/data/dataflow/BIS/WS_GLI/1.0/Q.TO1.5J.A.B.I.A.771?startPeriod={past_yr}&endPeriod={today}&format=csv"]
        # to download all
        # urls = ["https://stats.bis.org/api/v2/data/dataflow/BIS/WS_GLI/1.0/Q.TO1.5J.A.B.I.A.771?format=csv"]
        df = pd.concat([pd.read_csv(url) for url in urls])
        df.to_excel(self.file_dir + '/BIS Liquidity data.xlsx', index=False)
        # df2 = df[['TIME_PERIOD', 'OBS_VALUE']]
        # return df, df2
        path = f'{self.file_dir}/BIS Liquidity data.xlsx'
        file_name = 'BIS Liquidity data'
        print('BIS Liquidity data download completed...')
        return path, file_name 

    def imf_gdp_annual(self):

        '''IMF GDP Annual data'''
        
        href="https://api.worldbank.org/v2/en/indicator/NY.GNP.ATLS.CD?downloadformat=csv" 
        response = requests.get(href)
        zip_file_path = os.path.join(self.file_dir, 'IMF GDP Annual downloaded file.zip')
        
        with open(zip_file_path, 'wb') as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_file_path, 'r') as zip:
            zip.extractall(self.file_dir)   

        imf_gdp_annual_file_name = os.listdir(self.file_dir)[0]

        imf_data = pd.read_csv(self.file_dir +f'/{imf_gdp_annual_file_name}', skiprows=4)

        start_year = datetime.now().year - 11
        # years = ','.join([str(start_year + x) for x in range(10)])

        imf_data3 = imf_data.set_index('Country Name')
        # applymap/map can be use for many column while apply only for one
        imf_data1 = imf_data3[[str(start_year + x ) for x in range(10)]].map(lambda x: f"{x:,.2f}")

        df = imf_data1.loc[["China", "Hong Kong SAR, China", "India", "Indonesia", "Korea, Rep.", "Malaysia", "Mongolia", "Philippines", "Singapore", "Thailand", "Viet Nam"]]

        # create a excel writer object
        with pd.ExcelWriter(self.file_dir+ "/IMF GDP Annual.xlsx") as writer:        
            # use to_excel function and specify the sheet_name and index 
            # to store the dataframe in specified sheet
            df.to_excel(writer, sheet_name="selected country", index=True)
            imf_data3.to_excel(writer, sheet_name="all country", index=True)

        path = f'{self.file_dir}/IMF GDP Annual.xlsx'
        file_name = 'IMF GDP Annual'
        print('IMF GDP Annual data download completed...')
        return path, file_name

    def epu(self):

        '''EPU data'''

        urls = [f"https://www.policyuncertainty.com/media/All_Country_Data.xlsx"]

        df = pd.concat([pd.read_excel(url) for url in urls])
        df.to_excel(self.file_dir + '/EPU data.xlsx', index=False)
        path = f'{self.file_dir}/EPU data.xlsx'
        file_name = 'EPU data'
        print('EPU data download completed...')
        return path , file_name

    def vix_history(self):

        '''VIX_History data'''

        file_path = os.path.join(self.file_dir, 'VIX_History.csv')

        url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
        response = requests.get(url)
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
        df = pd.read_csv(self.file_dir + '/VIX_History.csv')
        df['DATE'] = pd.to_datetime(df['DATE']).dt.date
        df2 = df.copy()
        df2['Month'] = df2['DATE'].map(lambda x:int(x.month))
        df2['Year'] = df2['DATE'].map(lambda x:x.year)
        df2 = df2[['Month', 'Year', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
        # df3 = sqldf('select Month, Year, avg(OPEN), avg(HIGH), avg(LOW), avg(CLOSE)')

        with pd.ExcelWriter(self.file_dir + "/VIX_History.xlsx") as writer:        
            # use to_excel function and specify the sheet_name and index 
            # to store the dataframe in specified sheet
            df.to_excel(writer, sheet_name="daily", index=False)
            df2.to_excel(writer, sheet_name="average daily by month", index=False)

        path = f'{self.file_dir}/VIX_History.xlsx'
        file_name = 'VIX_History'
        print('VIX_History data download completed...')
        return path, file_name
    
    def zip_file(self):

        '''Create object of ZipFile
            src: https://www.tutorialspoint.com/how-to-create-a-zip-file-using-python'''
        
        if os.path.exists("download.zip"):
            os.remove("download.zip")
        with zipfile.ZipFile('download.zip', 'w') as zip_object:
        # Traverse all files in directory
            for folder_name, sub_folders, file_names in os.walk(self.file_dir):
                for filename in file_names:
                    # Create filepath of files in directory
                    file_path = os.path.join(folder_name, filename)
                    # Add files to zip file
                    zip_object.write(file_path, os.path.basename(file_path))
    
if __name__ == '__main__':
    list_folder(delete=True)
    print('Initiate data download...')
    all_data = DataCompile()
    all_data.clear_dir()
    path1, file_name1 = all_data.bis_liquidity()
    path2, file_name2 = all_data.imf_gdp_annual()
    path3, file_name3 = all_data.epu() 
    path4, file_name4 = all_data.vix_history()
    all_data.zip_file()
    last_download_datetime = datetime.today()
    details = {
    'Last Update': [last_download_datetime]
    }

    df = pd.DataFrame(details)
    all_data.clear_dir2()
    df.to_csv(f'{os.getcwd()}/last download/last_download.csv')
    paths = [path1, path2, path3, path4, 'download.zip', f'{os.getcwd()}/last download/last_download.csv']
    file_names = [file_name1,file_name2,file_name3,file_name4, 'download', 'last_download']
    
    
    upload_file(paths, file_names)

