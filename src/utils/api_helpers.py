"""
FRED API Helper Functions
"""

import requests
import os
from typing import Optional, Dict, Any


FRED_BASE_URL = "https://api.stlouisfed.org/fred"


def get_fred_api_key(secret_scope: str = "fred-api", secret_key: str = "api-key") -> Optional[str]:
    """
    Retrieve FRED API key from Databricks secrets or environment variable.
    """
    try:
        api_key = dbutils.secrets.get(scope=secret_scope, key=secret_key)
        print(f"✓ API key retrieved from secret scope: {secret_scope}")
        return api_key
    except NameError:
        api_key = os.environ.get("FRED_API_KEY")
        if api_key:
            print("✓ API key retrieved from environment variable")
        else:
            print("⚠ No FRED API key found")
        return api_key


def make_fred_request(
    endpoint: str,
    api_key: str,
    params: Dict[str, Any] = None,
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Make a request to the FRED API.
    """
    url = f"{FRED_BASE_URL}/{endpoint}"
    
    request_params = {
        "api_key": api_key,
        "file_type": "json"
    }
    
    if params:
        request_params.update(params)
    
    response = requests.get(url, params=request_params, timeout=timeout)
    response.raise_for_status()
    
    return response.json()
