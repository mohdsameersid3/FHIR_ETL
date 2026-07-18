from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


class Settings:

    def __init__(self):

        with open(CONFIG_PATH, "r") as file:
            self.config = yaml.safe_load(file)

    def get(self):
        return self.config

settings = Settings()


