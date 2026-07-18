from pathlib import Path

class FileUtils:

    @staticmethod
    def list_json_files(folder):
        
        folder = Path(folder)
        return sorted(folder.rglob("*.json"))