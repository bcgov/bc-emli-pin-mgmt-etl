FROM python:3.9 
# Or any preferred Python version.
ADD ./sftp /
RUN pip install paramiko
CMD ["python", "./sftp.py"] 
# Or enter the name of your unique directory and parameter set.