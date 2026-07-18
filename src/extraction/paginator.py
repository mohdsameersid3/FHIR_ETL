import requests

class Paginator:

    def __init__(self, client):
        self.client = client

    def fetch_pages(self, resource, params=None):

        url = f"{self.client.base_url.rstrip('/')}/{resource.lstrip('/')}"
        while url:
            response = requests.get(
                url,
                params=params,
                timeout=self.client.timeout
            )
            response.raise_for_status()
            bundle = response.json()
            yield bundle
            params = None
            url = None
            for link in bundle.get("link", []):
                if link["relation"] == "next":
                    url = link["url"]
                    break
