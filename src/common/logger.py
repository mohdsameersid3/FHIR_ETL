import logging
from pathlib import Path
from config.settings import settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_PATH = PROJECT_ROOT / Path(settings.get()["logging"]["log_path"])
LOG_PATH.mkdir(parents=True, exist_ok=True)

class LoggerFactory:

    @staticmethod
    def get_logger(module_name):
        module_name = module_name.split(".")[-1]
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
