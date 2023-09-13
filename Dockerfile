FROM python:3.9
ADD . /
RUN mkdir data
RUN chmod g+w ./data
RUN pip install -r requirements.txt
USER 1001
CMD python etl.py --host=${sftp_host} --port=${sftp_port} --username=${sftp_username} --password=${sftp_password} --remote_path=${remote_path} --local_path=${local_path}