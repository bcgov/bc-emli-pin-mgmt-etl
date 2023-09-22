import os
import sys
import logging

def setup_logging(log_folder, log_filename):
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_path = os.path.join(log_folder, log_filename)

    # Create a logger instance
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Configure the formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create a handler to write log messages to the specified log file
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    # Create a handler to display log messages in the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Redirect stdout and stderr to the logger
    sys.stdout = LoggerStream(logger, logging.INFO)
    sys.stderr = LoggerStream(logger, logging.ERROR)

class LoggerStream:
    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

# Example usage:
if __name__ == "__main__":
    log_folder = "/path/to/log_folder"  # Replace with the desired log folder path
    log_filename = "your_log_file.log"  # Replace with the desired log file name
    setup_logging(log_folder, log_filename)

    # You can now use the logger to log messages
    logger = logging.getLogger(__name__)
    logger.info("This is an example log message.")
    logger.error("This is an example error message.")

    # Messages printed with print() will also be captured and displayed in the console
    print("This is a printed message.")
