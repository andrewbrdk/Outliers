import requests

API_URL = "http://localhost:9090/api/update"
API_KEY = "your_api_key"

payload = {
    "id": [0, 1],            
    "title": ["Wiki Pageviews Threshold"]
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
        print(f"HTTP {response.status_code} error:")
        print(response.text.strip())
        return
    except Exception as e:
        print(f"Request error: {e}")
        return

    data = None
    try:
        data = response.json()
    except ValueError:
        print(response.text.strip())
        return

    print(data)

if __name__ == "__main__":
    main()