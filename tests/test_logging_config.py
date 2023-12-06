from unittest.mock import patch
from utils.logging_config import setup_logging, LoggerStream
import logging
import os

logFolder = ""
logFileName = "log_file.log"

self = {"logger": logging.getLogger("test_logger"), "log_level": 1, "linebug": ""}
buf = "Test log line 1\nTest log line 2   "


@patch("os.makedirs")
def test_setup_logging(makedirs_mock):
    setup_logging(logFolder, logFileName)
    os.remove(logFileName)
    assert makedirs_mock.called_once()


def test_logger_stream_write():
    LoggerStream.write(self, buf)


def test_logger_stream_flush():
    LoggerStream.flush(self)
