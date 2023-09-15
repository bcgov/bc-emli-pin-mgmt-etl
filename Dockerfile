FROM python:3.9
ADD . /
RUN mkdir data
RUN chmod g+w ./data
RUN pip install -r requirements.txt
USER 1001
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