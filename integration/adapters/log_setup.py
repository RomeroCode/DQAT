import logging
import os


# Logging configuration
log_directory = "monitoring/logs/"
os.makedirs(log_directory, exist_ok=True)

def setup_logger(name, log_file, level=logging.ERROR):
    """Configures logger to specific file

    Args:
        name (str): logger name
        log_file (str): logger path
        level (logging.level): log level
    
    Returns:
        logging.Logger: a logger instance configured
    """
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Creates File Handler
    log_file_path_header = os.path.join(log_directory, log_file)
    handler = logging.FileHandler(log_file_path_header)
    handler.setFormatter(formatter)

    # Creates and configures logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger