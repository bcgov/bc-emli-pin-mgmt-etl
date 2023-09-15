# Use an official Python 3.9 runtime as the base image
FROM python:3.9

# Add the current directory contents into the container at the root directory
ADD . /

# Create a data directory and grant group write permissions
RUN mkdir data && chmod g+w ./data

# Install Python dependencies from requirements.txt
RUN pip install -r requirements.txt

# Set the user to non-root (user ID 1001)
USER 1001

# Define the command to run when the container starts, including data_rules_url
CMD python etl.py \
    --sftp_host=${sftp_host} \
    --sftp_port=${sftp_port} \
    --sftp_username=${sftp_username} \
    --sftp_password=${sftp_password} \
    --sftp_remote_path=${sftp_remote_path} \
    --sftp_local_path=${sftp_local_path} \
    --processed_data_path=${processed_data_path} \
    --db_name=${db_name} \
    --db_write_batch_size=${db_write_batch_size} \
    --db_host=${db_host} \
    --db_port=${db_port} \
    --db_username=${db_username} \
    --db_password=${db_password} \
    --data_rules_url=${data_rules_url}