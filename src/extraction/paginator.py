import requests
from src.common.logger import LoggerFactory

class Paginator:

    def __init__(self, client):
        self.client = client
        self.logger = LoggerFactory.get_logger(__name__)

    def fetch_pages(self, resource, params=None):

        url = f"{self.client.base_url.rstrip('/')}/{resource.lstrip('/')}"
        self.logger.info(f"fetching the {resource} from: {url}")
        while url:
            response = requests.get(
                url,
                params=params,
                timeout=self.client.timeout
            )
            response.raise_for_status()
            bundle = response.json()
            self.logger.info(f"received {len(bundle.get('entry',[]))} records from {resource}")
            yield bundle
            params = None
            url = None
            for link in bundle.get("link", []):
                if link["relation"] == "next":
                    url = link["url"]
                    break
