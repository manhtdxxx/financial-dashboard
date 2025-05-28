import logging
import os
from datetime import datetime

def get_logger(name="ETL_Runner"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # # Console handler
        # console_handler = logging.StreamHandler()  # output to the console
        # console_handler.setFormatter(formatter)
        # logger.addHandler(console_handler)

        # File handler (daily log file)
        log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_filename = f"etl_{datetime.now().strftime('%Y-%m-%d')}.log"

        file_handler = logging.FileHandler(os.path.join(log_dir, log_filename))  # write to a file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
