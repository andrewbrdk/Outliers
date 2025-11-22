### Outliers

Time-series outlier detector.

<div align="center">
    <img src="https://i.postimg.cc/XVsj9FBX/outliers3.png" alt="outliers" width="800">
</div>


Start the service at [http://localhost:9090](http://localhost:9090) with the following commands:

```bash
git clone https://github.com/andrewbrdk/Outliers
cd Outliers
wget -P ./dist https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/{uPlot.iife.min.js,uPlot.min.css}
wget -P ./dist https://cdn.jsdelivr.net/npm/flatpickr@4.6.13/dist/{flatpickr.min.js,flatpickr.min.css}
go get outliers
go build
./outliers
```

Docker Compose starts a playground for running examples.
Make sure to set the correct PostgreSQL connection string in the [config](https://github.com/andrewbrdk/Outliers/blob/9b6e32c874426e9839964bc070d393085b909ca2/outliers.toml#L4). 
```bash
docker compose up --build

# Load example data
python -m venv pyvenv
source pyvenv/bin/activate
pip install requests psycopg2-binary
python ./examples/wiki_pageviews.py --start_date 2024-01-01 --end_date 2025-11-01
```
- Outliers: [http://localhost:9090](http://localhost:9090)
- PostgreSQL: localhost:5432
- Pgweb: [http://localhost:8081](http://localhost:8081)


Optional environmental variables:
```bash
OUTLIERS_PORT=":9090"              # web server port
OUTLIERS_CONF="./outliers.toml"    # config file
OUTLIERS_PASSWORD=""               # web interface password
OUTLIERS_API_KEY=""                # API key, see ./examples/trigger_update.py    
```

Config example:
```toml
[[connections]]
title = "main_pg"
type = "postgres"
connection = "$PG_CONSTR"

[[notifications]]
title = "Slack"
type = "slack"
webhook_url = "$SLACK_WEBHOOK"

[[detectors]]
title = "En.wiki Pageviews"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
cron_schedule = "05 03 * * *"
plot_lookback = 90
detection_method = 'dev_from_mean'
avg_window = 30
percent = 10
```

Config options:
```toml
# Postgres Connection
[[connections]]
title = "main_pg"
type = "postgres"
connection = "postgres://pguser:password123@localhost:5432/outliers"
#connection = "$PG_CONSTR"  # prefix with $ to parse from the env
max_open_conns = 10         # 10 by default
max_idle_conns = 5          # 3 by default
conn_max_lifetime = "60m"   # 30 min by default

# Slack Notifications
[[notifications]]
title = "Slack Repeater"
type = "slack"
webhook_url = "$SLACK_WEBHOOK"

# Email Notifications
[[notifications]]
title = "Emails"
type = "email"
smtp_server_with_port = "smtp.gmail.com:587"
username = "$SMTP_USERNAME"
password = "$SMTP_PASSWORD"
from = "$SMTP_USERNAME"
common_recipients = [""]

# Threshold
# Outlier: val < lower || val > upper
# detection_method = 'threshold'
# At least one of the 'lower' or 'upper' boundaries must be specified.
[[detectors]]
title = "En.wiki Pageviews, Threshold"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
notify_emails = [""]
plot_lookback = 90
detection_method = 'threshold'
lower = 220000000
upper = 300000000

# Deviation from the Mean, Percent
# For each point, looks back over the previous 'avg_window' points and computes their average.
# Marks the current point as an outlier if it falls outside the interval: average * (1 +- percent/100).
title = "En.wiki Pageviews"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project = 'en.wikipedia'"
output = "wiki_pageviews_outliers"
cron_schedule = "10 5 * * *"
notify_emails = [""]
plot_lookback = 90
detection_method = 'dev_from_mean'
avg_window = 30
percent = 10

# N Standard Deviations from the Mean
# For each point, looks back over the previous 'avg_window' points and computes the average and standard deviation.
# Marks the current point as an outlier if it falls outside the interval: average +- 'sigma' * standard deviation.
[[detectors]]
title = "En.wiki Pageviews, n*Sigma"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
notify_emails = [""]
plot_lookback = 90
detection_method = 'dev_from_mean'
avg_window = 30
sigma = 3

# Deviation from the Mean, Period
# For each point, looks back over the previous 'avg_window' points.
# Selects every i*'period' point (i = 1, 2, ...) within 'avg_window'.
# Uses these points to compute the average.
[[detectors]]
title = "En.wiki Pageviews, Period"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
notify_emails = [""]
plot_lookback = 90
detection_method = 'dev_from_mean'
avg_window = 60
period = 7
percent = 7

# Interquartile Range (IQR)
# For each point, looks back over the previous 'iqr_window' points.
# Computes the interquartile range (IQR) between Q1 (25th percentile) and Q3 (75th percentile).
# Marks the current point as an outlier if it falls below 
# Q1 - 'iqr_range' * IQR or above Q3 + 'iqr_range' * IQR.
[[detectors]]
title = "En.wiki Pageviews, IQR"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
cron_schedule = "15 5 * * *"
notify_emails = [""]
plot_lookback = 90
detection_method = 'iqr'
iqr_window = 60
iqr_range = 1.5
```

