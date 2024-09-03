from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import os.path
import random
from ssl import SSLError
import time
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError, TimeoutError
from googleapiclient.http import BatchHttpRequest

# Set global variables
# TODO - Move these to a configuration file.

# Authentication
DEFAULTSCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
CREDENTIALS_FILEPATH = 'credentials.json'   # The path to the credentials file.
TOKEN_FILEPATH = 'token.json'   # The path to the token file.

# Google Drive folder IDs
SOURCE_FOLDER_ID = '1cpo-7jgKSMdde-QrEJGkGxN1QvYdzP9V'
DESTINATION_FOLDER_ID = '10Fk5Src0lCQDEUfNPgwG4cXYRG3uPL1_'

MAX_RECURSION_DEPTH = 20
BATCH_SIZE = 100  # Adjust based on API limits and performance
MAX_WORKERS = 5  # Adjust based on system

# Internal function to initialize the Google OAuth connection.
def _init_google_oauth(scopes: list = DEFAULTSCOPES) -> Credentials:
    '''
    Initialize the Google OAuth connection.

    Args:
        scopes: The list of scopes to request access to.
    
    Returns:
        The Google OAuth credentials and token.
    '''

    # Load the credentials from the file.
    creds = None
    if os.path.exists(TOKEN_FILEPATH):
        creds = Credentials.from_authorized_user_file(TOKEN_FILEPATH)

    # If there are no (valid) credentials available or existing scopes don't contain desired scope(s).
    if not creds or not creds.valid or not all(scope in creds.scopes for scope in scopes):
        # If only issue is that the credentials are expired, refresh them.
        if creds and creds.expired and creds.refresh_token and all(scope in creds.scopes for scope in scopes):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILEPATH, scopes
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds

# Internal function to list items in the specified Google Drive folder.
def _list_items(service: build, folder_id: str, depth: int = 0, recursive: Optional[bool] = None, retries = 5) -> list:
    '''
    Internal function to list items in the specified Google Drive folder.

    Args:
        service: The Google Drive API service.
        folder_id: The ID of the Google Drive folder to list.
        depth: The current depth when listing items recursively.
        recursive: A flag to indicate whether to list items recursively.

    Returns:
        A list of items in the source Google Drive folder.

    Raises:
        HttpError: An error occurred accessing the Google Drive API.
    '''

    if depth > MAX_RECURSION_DEPTH:
        print(f"Max recursion depth reached for folder: {folder_id}")
        return []

    items = []
    page_token = None
    query = f"'{folder_id}' in parents"

    # Retry loop to handle transient errors
    for attempt in range(retries):
        try:
            # While loop to handle pagination of returned items
            while True:
                response = (
                    service.files().list(
                        q = query,
                        spaces = "drive",
                        corpora = "user",
                        fields = "nextPageToken, files(name, id, mimeType)",
                        pageToken = page_token,
                    ).execute()
                )
                
                # Ensure the response is a dictionary (sometimes a string is returned?)
                if not isinstance(response, dict):
                    raise ValueError(f"Unexpected response type: {type(response)}")
                
                files = response.get('files', [])
                items.extend(files)

                # If recursive flag is set and max depth is not reached, call recursively on folders
                if recursive and depth < MAX_RECURSION_DEPTH:
                    for file in files:
                        if file['mimeType'] == 'application/vnd.google-apps.folder':
                            items.extend(_list_items(service, file['id'], depth + 1, recursive))

                # Check if there are more pages to retrieve
                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break
            
            return items

        except (HttpError, SSLError, TimeoutError, ValueError) as error:
            if isinstance(error, HttpError) and error.resp.status in [403, 500, 503]:
                if 'rateLimitExceeded' in error.content.decode('utf-8'):
                    print("Rate limit exceeded. Retrying after a delay...")
                else:
                    print(f"An HTTP error occurred: {error}. Retrying...")
            elif isinstance(error, SSLError):
                print(f"An SSL error occurred: {error}. Retrying...")
            elif isinstance(error, TimeoutError):
                print(f"A timeout occurred: {error}. Retrying...")
            elif isinstance(error, ValueError):
                print(f"An unexpected response type error occurred: {error}. Retrying...")
            else:
                print(f"An error occurred: {error}")
                break

            time.sleep((2 ** attempt) + random.random())

    raise TimeoutError(f"Failed to list items in folder {folder_id} after {retries} attempts")

# Internal function to copy items from the source Google Drive folder to the destination Google Drive folder.
def _copy_items(service: build, source_folder_id: str, destination_folder_id: str, depth: int = 0, recursive: Optional[bool] = None):
    '''
    Internal function to copy items from the source Google Drive folder to the destination Google Drive folder.

    Args:
        service: The Google Drive API service.
        source_folder_id: The ID of the source Google Drive folder.
        destination_folder_id: The ID of the destination Google Drive folder.
        depth: The current depth when copying items recursively.
        recursive: A flag to indicate whether to copy items recursively.

    Returns:
        A dictionary containing the number of files and folders copied.

    Raises:
        HttpError: An error occurred accessing the Google Drive API.
    '''
    
    if depth > MAX_RECURSION_DEPTH:
        print(f"Max recursion depth reached for folder: {source_folder_id}")
        return {'copied_folder_count': 0, 'copied_file_count': 0}

    items = _list_items(service, source_folder_id)
    copied_folder_count = 0
    copied_file_count = 0
    folder_id_map = {}

    # Callback function to handle each batch response
    def handle_batch_response(request_id, response, exception):
        nonlocal copied_file_count, copied_folder_count
        if exception:
            print(f"An error occurred: {exception}")
        else:
            if 'mimeType' in response and response['mimeType'] == 'application/vnd.google-apps.folder':
                copied_folder_count += 1
                folder_id_map[request_id] = response['id']
                print(f"Folder copied: {response['name']} with new ID: {response['id']}")
            else:
                copied_file_count += 1
                print(f"File copied: {response['name']} with new ID: {response['id']}")

    # Function to create a batch request for item copy operations
    def create_batch_request(items):
        batch = service.new_batch_http_request(callback = handle_batch_response)
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                folder_metadata = {
                    'name': item['name'],
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [destination_folder_id]
                }
                batch.add(service.files().create(body = folder_metadata, fields = 'id, name, mimeType'), request_id = item['id'])
            else:
                file_metadata = {
                    'name': item['name'],
                    'parents': [destination_folder_id]
                }
                batch.add(service.files().copy(fileId = item['id'], body = file_metadata, fields='id, name, mimeType'), request_id = item['id'])
        return batch

    # If number of items exceeds configured batch size, split items into chunks for processing
    if len(items) > BATCH_SIZE:
        item_chunks = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    else:
        item_chunks = [items]

    # Process each chunk in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers = MAX_WORKERS) as executor:
        futures = [executor.submit(create_batch_request(chunk).execute) for chunk in item_chunks]

        for future in as_completed(futures):
            try:
                future.result()
            except HttpError as error:
                if 'rateLimitExceeded' in error.content.decode('utf-8'):
                    print("Rate limit exceeded. Retrying after a delay...")
                    time.sleep(1)
                else:
                    print(f"An error occurred: {error}")

    # Ensure all futures have completed
    for future in futures:
        future.result()

    # Recursively copy items in the folder if requested
    if recursive and depth < MAX_RECURSION_DEPTH:
        print(f"Recursion enabled, checking for nested folders")
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                print(f"Item {item['name']} is a folder, copying nested items")
                new_folder_id = folder_id_map.get(item['id'])
                if new_folder_id:
                    print(f"New folder ID retrieved, copying nested items to folder: {new_folder_id}")
                    nested_copy = _copy_items(service, item['id'], new_folder_id, depth + 1, recursive = True)
                    copied_folder_count += nested_copy['copied_folder_count']
                    copied_file_count += nested_copy['copied_file_count']

    return {'copied_file_count': copied_file_count, 'copied_folder_count': copied_folder_count}

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
    total_count = len(items)
    folder_count = Counter([item['mimeType'] for item in items])['application/vnd.google-apps.folder']
    file_count = total_count - folder_count
    
    return {'file_count': file_count, 'folder_count': folder_count}

def count_source_child_items_by_folder() -> dict:
    '''
    Recursively count the number of files and folders under each subfolder in the source Google Drive folder.

    Returns:
        A dictionary containing the number of files and folders in each folder in the source Google Drive folder.
    '''

    creds = _init_google_oauth()
    service = build("drive", "v3", credentials = creds)

    # List the top-level items in the source folder and filter to folders
    items = _list_items(service, SOURCE_FOLDER_ID)
    folders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']

    # Count the number of files and folders in each folder
    folder_counts = {}
    for folder in folders:
        folder_items = _list_items(service, folder['id'], recursive = True)

        # Count the number of files and folders
        total_count = len(folder_items)
        folder_count = Counter([folder_item['mimeType'] for folder_item in folder_items])['application/vnd.google-apps.folder']
        file_count = total_count - folder_count

        folder_counts[folder['name']] = {'nested_file_count': file_count, 'nested_folder_count': folder_count}
        
    folder_counts['total_nested_folder_count'] = sum(folder['nested_folder_count'] for folder in folder_counts.values())

    return folder_counts

def copy_source_items_to_dest_folder() -> int:
    '''
    Copy all files from the source Google Drive folder to the destination Google Drive folder, including nested files and folders.

    Returns:
        The number of files and folders copied to the destination folder.
    '''

    creds = _init_google_oauth(scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.metadata.readonly'])
    service = build("drive", "v3", credentials = creds)

    # Start copying from the source folder to the destination folder
    copied_item_counts = _copy_items(service, SOURCE_FOLDER_ID, DESTINATION_FOLDER_ID)

    return copied_item_counts