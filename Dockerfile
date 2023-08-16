FROM python:3.9
ADD ./sftp /
RUN chmod g+w ./
RUN pip install paramiko
USER 1001
CMD ["python", "./sftp.py"]