from collections import Counter
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set global variables
# TODO - Move these to a configuration file.

# Authentication
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
CREDENTIALS_FILEPATH = 'credentials.json'   # The path to the credentials file.
TOKEN_FILEPATH = 'token.json'   # The path to the token file.

# Google Drive folder IDs
SOURCE_FOLDER_ID = '1cpo-7jgKSMdde-QrEJGkGxN1QvYdzP9V'
DESTINATION_FOLDER_ID = '10Fk5Src0lCQDEUfNPgwG4cXYRG3uPL1_'

# Internal function to initialize the Google OAuth connection.
def _init_google_oauth() -> Credentials:
    '''
    Initialize the Google OAuth connection.
    
    Returns:
        The Google OAuth credentials and token.
    '''

    # Load the credentials from the file.
    creds = None
    if os.path.exists(TOKEN_FILEPATH):
        creds = Credentials.from_authorized_user_file(TOKEN_FILEPATH)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILEPATH, SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds

# Internal function to list items in the source Google Drive folder.
def _list_items(service, folder_id) -> list:
    '''
    Internal function to list items in the specified Google Drive folder.

    Args:
        service: The Google Drive API service.
        folder_id: The ID of the Google Drive folder to list.

    Returns:
        A list of items in the source Google Drive folder.

    Raises:
        HttpError: An error occurred accessing the Google Drive API.
    '''

    try:
        files = []
        page_token = None
        query = f"'{folder_id}' in parents"

        while True:
            response = (
                service.files().list(
                    q = query,
                    spaces = "drive",
                    corpora = "user",
                    fields = "nextPageToken, files(id, mimeType)",
                    pageToken = page_token,
                ).execute()
            )
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

    except HttpError as error:
        print(f"An error occurred: {error}")
        files = None

    return files

def count_source_items_by_type() -> dict:
    '''
    Count the number of files and folders in the source Google Drive folder.

    Returns:
        A dictionary containing the number of files and folders in the source Google Drive folder.
    '''

    creds = _init_google_oauth()
    service = build("drive", "v3", credentials = creds)

    # List the items in the source folder
    items = _list_items(service, SOURCE_FOLDER_ID)

    # Count the number of files and folders
    total_items = len(items)

    # Extract the mime types from the items
    mime_types = [item['mimeType'] for item in items]
    counter = Counter(mime_types)
    folder_count = counter['application/vnd.google-apps.folder']
    file_count = total_items - folder_count
    
    return {'file_count': file_count, 'folder_count': folder_count}
