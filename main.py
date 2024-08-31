import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Internal function to initialize the Google OAuth connection.
def _init_google_oauth() -> Credentials:
    '''
    Initialize the Google OAuth connection.
    
    Returns:
        The Google OAuth credentials and token.
    '''

    # The scopes to request from Google.
    SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

    # The path to the credentials file.
    CREDENTIALS_FILE = 'credentials.json'

    # The path to the token file.
    TOKEN_FILE = 'token.json'

    # Load the credentials from the file.
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds

def main():
    '''
    Use the _init_google_oauth() function and list the first ten Drive files the user has access to.

    Returns:
        None
    '''

    try:
        creds = _init_google_oauth()
        
        service = build("drive", "v3", credentials=creds)

        # Call the Drive v3 API
        results = (
            service.files()
            .list(pageSize=10, fields="nextPageToken, files(id, name)")
            .execute()
        )
        items = results.get("files", [])

        if not items:
            print("No files found.")
            return
        print("Files:")
        for item in items:
            print(f"{item['name']} ({item['id']})")
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")
