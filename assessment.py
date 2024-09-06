from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os.path
from pathlib import Path
import random
from ssl import SSLError
import time
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

# Retrieve JSON config file.
root_dir = Path(Path(__file__).parent)
with open("config.json", encoding = "utf8") as json_data_file:
    config = json.load(json_data_file)

# Parse config and initialize global variables
# Authentication
CREDENTIALS_FILEPATH = config["authentication"]["credentials_filepath"]
TOKEN_FILEPATH = config["authentication"]["token_filepath"]
DEFAULTSCOPES = config["authentication"]["default_scopes"]

# Google Drive folder IDs
SOURCE_FOLDER_ID = config["drive"]["source_folder_id"]
DESTINATION_FOLDER_ID = config["drive"]["destination_folder_id"]

# Performance
MAX_RECURSION_DEPTH = config["performance"]["max_recursion_depth"]
MAX_RETRIES = config["performance"]["max_retries"]
BATCH_SIZE = config["performance"]["batch_size"]
MAX_WORKERS = config["performance"]["max_workers"]

# Logging
LOG_FILE_ENABLED = config["logging"]["log_file_enabled"]
LOG_FILE_PATH = config["logging"]["log_file_path"]
LOG_LEVEL_CONSOLE = getattr(logging, (config["logging"]["log_level_console"]).upper(), logging.INFO)
LOG_LEVEL_FILE = getattr(logging, (config["logging"]["log_level_file"]).upper(), logging.DEBUG)
LOG_FORMAT = config["logging"]["log_format"]

# Create logger and console handler
logger = logging.getLogger('assessment')
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL_CONSOLE)
formatter = logging.Formatter(LOG_FORMAT)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# If enabled, create file handler
if LOG_FILE_ENABLED:
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setLevel(LOG_LEVEL_FILE)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Internal function to initialize the Google OAuth connection.
def _init_google_oauth(scopes: list = DEFAULTSCOPES) -> Credentials:
    '''
    Internal function to initialize the Google OAuth connection using the provided credentials/token.

    Args:
        scopes: The list of scopes to request access to.
    
    Returns:
        The Google OAuth credentials and token.

    Raises:
        Exception: An error occurred during the OAuth flow.
        Exception: An error occurred while saving the token.
    '''

    # Load the credentials from the file.
    creds = None
    if os.path.exists(TOKEN_FILEPATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILEPATH)
        except Exception as e:
            logger.error(f"An error occurred while loading existing token: {e}")
            raise

    # If there are no (valid) credentials available or existing scopes don't contain desired scope(s).
    if not creds or not creds.valid or not all(scope in creds.scopes for scope in scopes):
        # If only issue is that the credentials are expired, refresh them.
        if creds and creds.expired and creds.refresh_token and all(scope in creds.scopes for scope in scopes):
            creds.refresh(Request())
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILEPATH, scopes
                )
                creds = flow.run_local_server(port=0)
            except Exception as e:
                logger.error(f"An error occurred during OAuth flow: {e}")
                raise
        
        # Save the credentials for the next run
        try:
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        except Exception as e:
            logger.error(f"An error occurred while saving your token: {e}")
            raise
    
    return creds

# Internal function to list items in the specified Google Drive folder.
def _list_items(service: build, folder_id: str, depth: int = 0, recursive: Optional[bool] = None) -> list:
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
        SSLError: An SSL error occurred.
        TimeoutError: A timeout occurred while accessing the Google Drive API.
        ValueError: An unexpected response type was received.
    '''

    if depth > MAX_RECURSION_DEPTH:
        logger.warning(f"Max recursion depth reached for folder: {folder_id}")
        return []

    items = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed=false"

    # Retry loop to handle transient errors
    for attempt in range(MAX_RETRIES):
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
                    logger.error("Rate limit exceeded. Retrying after a delay...")
                else:
                    logger.error(f"An HTTP error occurred: {error}. Retrying...")
            elif isinstance(error, SSLError):
                logger.error(f"An SSL error occurred: {error}. Retrying...")
            elif isinstance(error, TimeoutError):
                logger.error(f"A timeout occurred: {error}. Retrying...")
            elif isinstance(error, ValueError):
                logger.error(f"An unexpected response type error occurred: {error}. Retrying...")
            else:
                logger.error(f"An error occurred: {error}")
                break

            time.sleep((2 ** attempt) + random.random())

    raise TimeoutError(f"Failed to list items in folder {folder_id} after {MAX_RETRIES} attempts")

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
        logger.warning(f"Max recursion depth reached for folder: {source_folder_id}")
        return {'copied_folder_count': 0, 'copied_file_count': 0}

    items = _list_items(service, source_folder_id)
    copied_folder_count = 0
    copied_file_count = 0
    folder_id_map = {}

    # Callback function to handle each batch response
    def handle_batch_response(request_id, response, exception):
        nonlocal copied_file_count, copied_folder_count
        if exception:
            logger.error(f"An error occurred: {exception}")
        else:
            if 'mimeType' in response and response['mimeType'] == 'application/vnd.google-apps.folder':
                copied_folder_count += 1
                folder_id_map[request_id] = response['id']
                logger.info(f"Folder copied: {response['name']} with new ID: {response['id']}")
            else:
                copied_file_count += 1
                logger.info(f"File copied: {response['name']} with new ID: {response['id']}")

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
                    logger.warning("Rate limit exceeded. Retrying after a delay...")
                    time.sleep(1)
                else:
                    logger.error(f"An error occurred: {error}")

    # Ensure all futures have completed
    for future in futures:
        future.result()

    # Recursively copy items in the folder if requested
    if recursive and depth < MAX_RECURSION_DEPTH:
        logger.debug(f"Recursion enabled, checking for nested folders")
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                logger.debug(f"Item {item['name']} is a folder, copying nested items")
                new_folder_id = folder_id_map.get(item['id'])
                if new_folder_id:
                    logger.debug(f"New folder ID retrieved, copying nested items to folder: {new_folder_id}")
                    nested_copy = _copy_items(service, item['id'], new_folder_id, depth + 1, recursive = True)
                    copied_folder_count += nested_copy['copied_folder_count']
                    copied_file_count += nested_copy['copied_file_count']

    return {'copied_file_count': copied_file_count, 'copied_folder_count': copied_folder_count}

def _copy_items_bfs(service, source_folder_id, destination_folder_id):
    '''
    Internal function to copy items from the source Google Drive folder to the destination Google Drive folder, using a breadth-first search (BFS) approach.

    Args:
        service: The Google Drive API service.
        source_folder_id: The ID of the source Google Drive folder.
        destination_folder_id: The ID of the destination Google Drive folder.
        
    Returns:
        A dictionary containing the number of files and folders copied.

    Raises:
        HttpError: An error occurred accessing the Google Drive API.
    '''

    copied_folder_count = 0
    copied_file_count = 0

    # Initialize a double-ended queue with the top-level source and destination folder IDs
    queue = deque([tuple((source_folder_id, destination_folder_id))])

    # Process top-level and all nested folders one level at a time
    while queue:
        # Store the length (breadth) of the current level before we start processing
        level_breadth = len(queue)
        logger.debug(f"----Starting new level with breadth {level_breadth} ----")

        # Reset folder_id_map for each level
        folder_id_map = {}

        # Callback function to handle each batch request response
        def handle_batch_response(request_id, response, exception):
            nonlocal copied_file_count, copied_folder_count
            if exception:
                logger.error(f"An error occurred: {exception}")
            else:
                # Check if the response is a folder or file and update counts
                if 'mimeType' in response and response['mimeType'] == 'application/vnd.google-apps.folder':
                    copied_folder_count += 1
                    logger.info(f"Folder copied: {response['name']} with new ID: {response['id']}")
                    
                    # Store mapping of source folder ID to new folder ID (to add to queue later)
                    folder_id_map[request_id] = response['id']
                else:
                    copied_file_count += 1
                    logger.info(f"File copied: {response['name']} with new ID: {response['id']}")

        # Function to create a batch request for all items in a given folder
        def create_batch_request(items, parent_id):
            batch = service.new_batch_http_request(callback = handle_batch_response)
            for item in items:
                # For each item, add either a folder creation request or a copy request to the batch
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    folder_metadata = {
                        'name': item['name'],
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [parent_id]
                    }
                    batch.add(service.files().create(body = folder_metadata, fields = 'id, name, mimeType'), request_id = item['id'])
                else:
                    file_metadata = {
                        'name': item['name'],
                        'parents': [parent_id]
                    }
                    batch.add(service.files().copy(fileId = item['id'], body = file_metadata, fields = 'id, name, mimeType'), request_id = item['id'])
            return batch

        # Process folders at the current level in parallel
        with ThreadPoolExecutor(max_workers = min(MAX_WORKERS, level_breadth)) as executor:
            futures = []
            
            # For each folder, retrieve its items and create a batch request to copy them
            for _ in range(level_breadth):
                src_id, dest_id = queue.popleft()
                print (f"Processing source folder {src_id} and destination folder {dest_id}")
                items = _list_items(service, src_id)
                if items:
                    logger.debug(f"Found {len(items)} items in folder {src_id}, creating batch requests.")
                    for i in range(0, len(items), BATCH_SIZE):
                        chunk = items[i:i + BATCH_SIZE]
                        batch = create_batch_request(chunk, dest_id)
                        futures.append(executor.submit(batch.execute))

            # Wait for all batch requests to complete and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    logger.error(f"An unexpected error occurred: {error}")

        # Ensure all futures have completed
        for future in futures:
            try:
                future.result()
            except Exception as error:
                logger.error(f"An error occurred while ensuring futures completion: {error}")

        # Debug output to verify folder_id_map contents
        logger.debug(f"folder_id_map: {folder_id_map}")

        # Add nested folders that were just created to the queue for processing of the next level down
        queue.extend((src_id, new_dest_id) for src_id, new_dest_id in folder_id_map.items())

    return {'copied_file_count': copied_file_count, 'copied_folder_count': copied_folder_count}

def count_source_items_by_type() -> dict:
    '''
    Count the number of files and folders at the root of the source Google Drive folder.

    Returns:
        A dictionary containing the number of files and folders at the root of the source Google Drive folder.
    '''
    logger.info("Assessment #1 - Counting files and folders at the root of the source folder...")
    logger.debug(f"Source folder ID: {SOURCE_FOLDER_ID}")
    logger.debug(f"Destination folder ID: {DESTINATION_FOLDER_ID}")

    creds = _init_google_oauth()
    service = build("drive", "v3", credentials = creds)

    # List the items in the source folder
    items = _list_items(service, SOURCE_FOLDER_ID)

    # Count the number of files and folders
    total_count = len(items)
    folder_count = Counter([item['mimeType'] for item in items])['application/vnd.google-apps.folder']
    file_count = total_count - folder_count
    
    logger.info(f"Total items: {total_count} - Files: {file_count}, Folders: {folder_count}")
    return {'file_count': file_count, 'folder_count': folder_count}

def count_source_child_items_by_folder() -> dict:
    '''
    Recursively count the number of files and folders under each subfolder in the source Google Drive folder.

    Returns:
        A dictionary containing the number of files and folders in each folder in the source Google Drive folder and the total count of nested folders under the source folder.
    '''
    logger.info("Assessment #2 - Counting child objects in each subfolder under the source folder...")
    logger.debug(f"Source folder ID: {SOURCE_FOLDER_ID}")
    logger.debug(f"Destination folder ID: {DESTINATION_FOLDER_ID}")

    creds = _init_google_oauth()
    service = build("drive", "v3", credentials = creds)

    # List the top-level items in the source folder and filter to folders
    items = _list_items(service, SOURCE_FOLDER_ID)
    folders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']

    # Count the number of files and folders in each folder
    folder_counts = {}
    for folder in folders:
        logger.debug(f"Processing folder: {folder['name']}...")
        folder_items = _list_items(service, folder['id'], recursive = True)

        # Count the number of files and folders
        total_count = len(folder_items)
        folder_count = Counter([folder_item['mimeType'] for folder_item in folder_items])['application/vnd.google-apps.folder']
        file_count = total_count - folder_count

        # Store the counts for the current folder
        logger.info(f"Folder: {folder['name']} - Child Files: {file_count}, Child Folders: {folder_count}")
        folder_counts[folder['name']] = {'nested_file_count': file_count, 'nested_folder_count': folder_count}
        
    # Store the total count of all folders under the source folder (top-level folders + all nested folders)
    folder_counts['total_nested_folder_count'] = sum([len(folders)] + [folder['nested_folder_count'] for folder in folder_counts.values()])
    logger.info(f"Total nested folder count (including top-level): {folder_counts['total_nested_folder_count']}")

    return folder_counts

def copy_source_items_to_dest_folder(bfs: Optional[bool] = None) -> int:
    '''
    Copy all files from the source Google Drive folder to the destination Google Drive folder, including nested files and folders.

    Args:
        bfs: A flag to indicate whether to copy items using a breadth-first search (BFS) approach. (EXPERIMENTAL)

    Returns:
        A dictionary containing the number of files and folders copied to the destination folder.
    '''
    logger.info("Assessment #3 - Copying the content of the source folder to the destination folder...")
    logger.debug(f"Source folder ID: {SOURCE_FOLDER_ID}")
    logger.debug(f"Destination folder ID: {DESTINATION_FOLDER_ID}")

    creds = _init_google_oauth(scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.metadata.readonly'])
    service = build("drive", "v3", credentials = creds)

    # Start copying from the source folder to the destination folder
    if bfs:
        logger.info("BFS flag set, using experimental breadth-first search method.")
        copied_item_counts = _copy_items_bfs(service, SOURCE_FOLDER_ID, DESTINATION_FOLDER_ID)
    else:
        logger.info("Using default recursive copy method (DFS).")
        copied_item_counts = _copy_items(service, SOURCE_FOLDER_ID, DESTINATION_FOLDER_ID, recursive = True)

    return copied_item_counts

if __name__ == "__main__":
    logger.info("Executed as script, running all three assessments...")
    count_source_items_by_type()
    count_source_child_items_by_folder()
    copy_source_items_to_dest_folder()