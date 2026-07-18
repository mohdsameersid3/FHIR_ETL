import logging
from pathlib import Path
from config.settings import settings

LOG_PATH = Path(settings.get()["logging"]["log_path"])
LOG_PATH.mkdir(parents=True, exist_ok=True)

class LoggerFactory:

    @staticmethod
    def get_logger(module_name):
        logger = logging.getLogger(module_name)
        if logger.handlers:
            return logger
        logger.setLevel(settings.get()["logging"]["level"])
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s")

        module_path = LOG_PATH / f"{module_name}.log"
        module_handler = logging.FileHandler(module_path)
        module_handler.setFormatter(formatter)
        logger.addHandler(module_handler)

        return logger
