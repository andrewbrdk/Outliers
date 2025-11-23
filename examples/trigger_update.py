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

    response_ids = set(data["update_started"].keys())
    response_titles = set(data["update_started"].values())
    payload_ids = set([str(i) for i in payload["id"]])
    payload_titles = set(payload["title"])
    all_present = payload_ids.issubset(response_ids) and payload_titles.issubset(response_titles)
    if data['status'] != 'ok' or not all_present:
        sys.exit(f"Failed to start updates for all detectors, {payload}")

if __name__ == "__main__":
    main()
