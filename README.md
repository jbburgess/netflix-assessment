# Netflix Technical Assessment - Jonathan Burgess

## Overview

An importable Python module, `assessment`, with functions designed to fulfull the requirements of the three parts of the overall Netflix Technical Assessment. The module is designed to be used locally on any workstation that supports recent versions of Python (tested with Python 3.11 and 3.12 on Windows 11 and macOS).

## Installation

1. Clone the repository.
2. Install the dependencies listed in `requirements.txt` (preferably via a new virtual environment).
3. Download and place the client secret JSON file for your GCP OAuth2 credential in the root directory of the project.
4. If needed, modify the `config.json` file to point to the proper path and filename for your credentials file. You can modify other settings if desired, but the functions should run successfully with the default settings (See [`Optional Configurations`](#optional-configurations) below for full details).

## Usage

The module can either be imported and the functions called individually, or it can be run directly from the command line as a script. To run the module from the command line, use the following command:

```bash
python -m assessment
```

This will execute all three assessments in order. If you want to run only one of the assessments, you can import the module in a Python session and call the individual functions:

### Assessments

> Complete the assessments below in any order. Structure your project in a way that allows evaluators to run your solution for each assessment individually. For assessments #1 and 2,
 output the results in any format that you’d like, but be prepared to explain why you chose a specific format. 

#### Assessment #1

> Write a script to generate a report that shows the number of files and folders in total at the root of the source folder.

To execute individually:

```python
>>> import assessment
>>> assessment.count_source_items_by_type()
```

The function will report human-readable results directly to the console and also return a `dict` structured as follows:

```python
{
    'file_count': 0,
    'folder_count': 0
}
```

#### Assessment #2

> Write a script to generate a report that shows the number of child objects (recursively) for each top-level folder under the source folder id and a total of nested folders for the source folder.

To execute individually:

```python
>>> import assessment
>>> assessment.count_source_child_items_by_folder()
```

The function will report human-readable results directly to the console and also return a `dict` structured as follows:

```python
{
    'Folder Name #1': {
        'nested_file_count': 0,
        'nested_folder_count': 0
    },
    'Folder Name #2': {
        'nested_file_count': 0,
        'nested_folder_count': 0
    },
    [...]
    'total_nested_folder_count': 0
}
```

#### Assessment #3

> Write a script to copy the content (nested files/folders) of the source folder to the destination folder.

To execute individually:

```python
>>> import assessment
>>> assessment.copy_source_items_to_dest_folder()
```

The function will report human-readable results directly to the console as it performs copy actions and also return a `dict` upon completion that's structured as follows:

```python
{
    'copied_file_count': 0,
    'copied_folder_count': 0,
}
```
> **Note:** As folders can't be copied in Google Drive and a new one must instead be created before copying any contained files into it, executing this function will not overwrite any existing files or folders in the destination folder with the same name. There will instead be a duplicate folder structure created from the top-level down.

### Authentication

The module uses OAuth2 for authentication to the Google API. The user must have a Google account and a GCP project with Google Drive API v3 enabled. The user must create an OAuth2 credential and download the JSON credentials file for use with this module. The path to this file should be specified in the `config.json` file (it defaults to being located at the root and named `credentials.json`).

Upon running the module for the first time, the user will be prompted to authenticate with Google Drive using the OAuth2 credentials and allow the app to access their account. The resulting authentication token will be stored in a file specified in the `config.json` file, and, rather than prompting for authentication each time, subsequent runs of the module will use this token to authenticate (and refresh it if needed).

Since the Google Drive API does not offer scopes that provide read/write access only to specific folders (unless you have a web-based frontend through which you can use the Picker API to choose and allow specific folders via the `drive.file` scope), the module may request access to *all* files and folders in the user's Google Drive account, depending on which assessment is being executed. The module is designed to only read and write to the source and destination folders specified in the `config.json` file, but the user should be aware that the module will have access to all files and folders in their Google Drive account and that they should review this code to their satisfaction before executing it.

> **Note:** To mitigate this somewhat, the module will only request full access (`drive`) if the user is executing assessment #3 and copying items between folders. The other two assessments do not require file read or write access and will instead request a read-only scope to file metadata (`drive.metadata.readonly`). If you have an existing token with just the metadata read-only scope, then you will be prompted to re-authenticate and provide the full read/write permissions if you attempt to execute assessment #3, and that token will replace the saved token you had previously.

### Optional Configurations

The included `config.json` file contains the following non-sensitive values that can be modified to customize the behavior of the module:

#### Authentication Settings

* `credentials_filepath`: Relative or full path to the file containing the downloaded JSON of the user's GCP OAuth2 credential.
  * Default value: `"credentials.json"`
* `token_filepath`: Relative or full path to the file where the authentication token is stored.
  * Default value: `"token.json"`
* `default_scopes`: List of scopes to request by default for Google OAuth authentication.
  * Default value: `["https://www.googleapis.com/auth/drive.metadata.readonly"]`

#### Drive Settings
* `source_folder_id`: The ID of the source folder in Google Drive.
  * Default value: `"1cpo-7jgKSMdde-QrEJGkGxN1QvYdzP9V"`
* `destination_folder_id`: The ID of the destination folder in Google Drive.
  * Default value: `"10Fk5Src0lCQDEUfNPgwG4cXYRG3uPL1_"`

#### Logging Settings
* `log_file_enabled`: Boolean value indicating whether logging to a file is enabled. Enabling this will output log messages to the path specified in `log_file_path`.
  * Default value: `"False"`
* `log_file_path`: Relative or full Path to the outputted log file.
  * Default value: `"log.txt"`
* `log_level_console`: Logging level for console output. Function results are 
  * Default value: `"INFO"`
* `log_level_file`: Logging level for file output. Set this to `INFO` and `log_file_enabled` to `true` to output report results to the path specified in `log_file_path`.
  * Default value: `"DEBUG"`
* `log_format`: Format string for log messages.
  * Default value: `"%(asctime)s - %(levelname)s - %(message)s"`
  > **Note:** Console reporting of assessment results is done at the `INFO` level, so narrowing console or file level settings will result in assessment reports not streaming to the respective outputs.

#### Performance Settings
* `max_recursion_depth`: Maximum depth for recursive file operations.
  * Default value: `20`
* `max_retries`: Maximum number of retries for failed `files().list()` operations.
  * Default value: `5`
* `batch_size`: Maximum number of API calls to process in a single batch operation.
  * Default value: `100`
* `max_workers`: Maximum number of worker threads for parallel processing.
  * Default value: `5`

These settings allow you to configure various aspects of the application, including authentication, Google Drive operations, logging, and performance. Adjust these settings as needed to fit your specific requirements.

## Assumptions

The following assumptions were made when developing the module:

1. The user has a Google account and GCP project with a valid OAuth2 token.
    a. The user has enabled the Google Drive API v3 on their GCP project.
    b. The user has created an OAuth2 credential and downloaded the JSON for it to this project directory.
2. The user has created a virtual environment and installed the dependencies.
4. The user has a basic understanding of loading and executing Python modules.
3. JSON files are well-formed and contain the correct keys.
4. JSON files are not being updated while the module is running.

## Improvements

The following improvements could be made to the module, depending on requirements and desired investment of time and resources:

1. Add a testing suite for the functions using `pytest` or `unittest`
    1. Add unit testing with proper mocking of the Google Drive API.
    2. Add integration testing that tests the functions against a test Google Drive account with known folder structures and files.
2. Add different options for interacting with and executing the assessments, depending on the targeted audience and use case. This could be anything from a command-line based menu that allows individual function calls while running the module as a script to a web-based frontend that uses the Picker API to allow the user to select the source and destination folders, allowing for more granular control over the permissions being granted to the app, and then execute the assessments and get reports via a GUI.
3. Add additional options for exporting the results of the assessments, such as writing the results to a CSV file, a Google Sheet, or a PDF report.
4. Improve performance of breadth-first search for file copy operations. Currently, the module defaults to a depth-first search, which can be slow for large folder structures. There's an optional implemention of BFS in the `copy_source_items_to_dest_folder()` function, but it's not used by default, as it results in frequent API timeout and SSL errors that are seemingly unrelated to rate limits (this functionality can be used by setting the `bfs` parameter to `True` in the function call).
5. Add additional functionality to the `copy_source_items_to_dest_folder()` function that allows for optional overwriting of existing files with the same name in the destination folder, with additional logic that checks for existing folders and files in the destination folder, compares Last Modified timestamps, allows for either updating existing files with new content revisions, leaving existing file/folder as-is, etc.

## Authors

- **[Jonathan Burgess](https://www.github.com/jbburgess)**