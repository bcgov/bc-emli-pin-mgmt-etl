# bc-emli-pin-mgmt-etl

![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)

# BC PVS ETL Job

This is an ETL (Extract, Transform, Load) job designed to expire PINS using data from LTSA. The job consists of three main steps: downloading data from an SFTP server, processing the downloaded data, and writing the processed data to a PostgreSQL database. Additionally, it sends email notifications using GC Notify to inform about the job's status and any errors encountered.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [License](#license)

## Prerequisites

Before running the BC PVS ETL Job, ensure you have the following prerequisites installed and configured:

- Python 3.x
- Pip (Python package manager)
- Access to an LTSA SFTP server with the necessary credentials
- Access to a PostgreSQL database with the necessary credentials
- A GC Notify API key for sending email notifications
- Data rules in the form of a `data_rules.json` file available via a public GitHub repository

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/bcgov/bc-emli-pin-mgmt-etl.git
   ```
   
2. Navigate to the project directory:
   ```
   cd bc-emli-pin-mgmt-etl
   ```

3. Install the required Python packages using pip:
   ```
   pip install -r requirements.txt
   ```

## Usage
To run the BC PVS ETL Job, use the following command:

```
python etl_job.py [OPTIONS]
```

Replace `[OPTIONS]` with the necessary command-line arguments based on your specific configuration. See the Configuration section below for details on available options.

## Configuration

The BC PVS ETL Job supports various configuration options through command-line arguments. Below is a list of available options and their descriptions:

```
--sftp_host: Host address of the LTSA SFTP server.
--sftp_port: Port number of the LTSA SFTP server (default: 22).
--sftp_username: Username for SFTP login.
--sftp_password: Password for SFTP login.
--sftp_remote_path: Remote path of the SFTP folder to copy files from.
--sftp_local_path: Local folder path to download files to.
--processed_data_path: Local output folder for processed CSV files.
--db_host: Host name of the PostgreSQL database.
--db_port: Port number of the PostgreSQL database (default: 5432).
--db_username: Username for database login.
--db_password: Password for database login.
--db_name: Name of the database in the PostgreSQL DB.
--db_write_batch_size: Number of records to write to the database in one batch (default: 1000).
--data_rules_url: URL to the data_rules.json file in a public GitHub repository.
--api_key: Your GC Notify API key for sending email notifications.
--base_url: The base URL of the GC Notify API.
--email_address: The recipient's email address for notifications.
--template_id: The ID of the email template to use for notifications.
```

Please ensure you have the necessary credentials and configurations for the LTSA SFTP server, PostgreSQL database, and GC Notify to successfully run the ETL job.

## License

Copyright 2023 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
