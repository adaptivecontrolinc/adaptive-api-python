import requests
from typing import Any, Dict, Dict, Optional

class BaseApi:
    def __init__(self, server: str, token: str, api_type: str):
        self.base_url = f"{server.rstrip('/')}/api/v1/{api_type}"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": f"AdaptiveApi{api_type.capitalize()}/1.0"
        })

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _fetch(self, path: str, query: Optional[dict] = None) -> Any:
        url = self._url(path)
        response = self.session.get(url, params=query, timeout=10)
        response.raise_for_status()
        return response.json()



    def _post(self, path: str, params: Optional[Dict[str, Any]] = None, 
                    body: Optional[str] = None) -> Any:
        url = self._url(path)
        response = self.session.post(url, params=params, json=body, timeout=10)
        response.raise_for_status()
        return response.json()