from src.common.logger import LoggerFactory
from config.settings import settings
import requests


class FHIRClient:

    def __init__(self):

        config = settings.get()

        self.base_url = config['api']['base_url']
        self.timeout = config['api']['timeout']
        # self.logger = LoggerFactory.get_logger(__name__)

