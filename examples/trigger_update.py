import sys
import requests

API_URL = "http://localhost:9090/api/update"
API_KEY = "your_api_key"

payload = {
    "id": [0, 1],            
    "title": ["En.wiki Pageviews, Threshold"]
}

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def main():
    try:
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        sys.exit(f"HTTP {response.status_code} error: {response.text}")
    except Exception as e:
        sys.exit(f"Request error: {e}")

    data = None
    try:
        data = response.json()
    except ValueError:
        sys.exit(response.text)

    print(data)

if __name__ == "__main__":
    main()
