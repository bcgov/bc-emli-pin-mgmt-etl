FROM python:3.9
ADD ./sftp /
RUN mkdir data
# RUN chmod g+w ./data
RUN pip install paramiko
# USER 1001
CMD ["python", "./sftp.py"]