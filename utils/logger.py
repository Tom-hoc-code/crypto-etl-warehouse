import logging
import os
from datetime import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name: str, level=logging.INFO):
    """
    Tạo logger chuẩn dùng cho toàn project
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Tránh log bị duplicate khi import nhiều lần
    if logger.handlers:
        return logger

    # Format log
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Log ra console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Log ra file theo ngày
    log_file = os.path.join(
        LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
