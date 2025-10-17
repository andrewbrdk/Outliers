import sys
import requests
import argparse
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

PGCON = {
    'host': 'localhost', # locally
    #'host': 'clickhouse', # docker
    'port': 5432,
    'database': 'outliers',
    'user': 'pguser', 
    'password': 'password123'
}

# https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/concepts/page-views.html
# https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html

wikiproject = 'en.wikipedia'
base_url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate/{wikiproject}/all-access/user/daily/{start_date}/{end_date}"

# https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}

CREATE_WIKI_PAGEVIEWS = """
CREATE TABLE IF NOT EXISTS wiki_pageviews (
    dt DATE NOT NULL,
    project TEXT NOT NULL,
    views INTEGER NOT NULL,
    PRIMARY KEY (dt, project)
);
"""

DELETE_FROM_WIKI_PAGEVIEWS = """
    DELETE FROM wiki_pageviews
    WHERE
        project = '{wikiproject}'
        and dt >= '{start_date}'
        and dt <= '{end_date}'
"""

INSERT_WIKI_PAGEVIEWS = """
INSERT INTO wiki_pageviews (dt, project, views)
VALUES %s
ON CONFLICT (dt, project) DO UPDATE SET views = EXCLUDED.views;
"""

def main():
    parser = argparse.ArgumentParser(description="Get Wikipedia pageviews")
    parser.add_argument("--end_date", required=True, type=str, help="End date in YYYY-MM-DD")
    parser.add_argument("--start_date", type=str, help="Start date in YYYY-MM-DD (optional). start_date=end_date-30 if not specified.")
    args = parser.parse_args()
    
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = (end_date - timedelta(days=30))
    end_date = end_date.strftime('%Y%m%d')
    start_date = start_date.strftime('%Y%m%d')

    try:
        response = requests.get(
            base_url.format(wikiproject=wikiproject, start_date=start_date, end_date=end_date), 
            headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)

    views = [
        (datetime.strptime(item['timestamp'],'%Y%m%d00').date(), item['project'], item['views']) 
        for item in data['items']
    ]

    print('Pageviews:')
    print(views)

    try:
        conn = psycopg2.connect(**PGCON)
        cur = conn.cursor()
        cur.execute(CREATE_WIKI_PAGEVIEWS)
        cur.execute(DELETE_FROM_WIKI_PAGEVIEWS.format(wikiproject=wikiproject, start_date=start_date, end_date=end_date))
        execute_values(cur, INSERT_WIKI_PAGEVIEWS, views)
        conn.commit()
        cur.close()
        conn.close()
        print("Data successfully written to PostgreSQL.")
    except Exception as e:
        print(f"Error writing to DB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
