FROM python:3.9
ADD . /
RUN mkdir data
RUN chmod g+w ./data
RUN pip install -r requirements.txt
USER 1001

# Environment variables
# ENV sftp_host "test.rebex.net"
# ENV sftp_port 22
# ENV sftp_username "demo"
# ENV sftp_password "password"

CMD python etl.py --host=${sftp_host} --port=${sftp_port} --username=${sftp_username} --password=${sftp_password} --remote_path=${remote_path} --local_path=${local_path} --processed_data_path=${processed_data_path}