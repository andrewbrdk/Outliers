### Outliers

Timeseries outliers detector.

<div align="center">
    <img src="https://i.postimg.cc/GrBj3QTD/outliers.png" alt="outliers" width="700">
</div>

The service starts at [http://localhost:9090](http://localhost:9090) after the following commands:

```bash
git clone https://github.com/andrewbrdk/Outliers
cd Outliers
go get outliers
go build
./outliers
```

Optional environmental variables:
```bash
OUTLIERS_PORT=":9090"                    # web server port
OUTLIERS_CONF="./outliers.toml"          # config file
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
title = "Wiki Pageviews"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
cron_schedule = "05 03 * * *"
detection_method = 'dist_from_mean'
percent = 10
avg_window = 30
```

Config options:
```toml
# Postgres Connection
[[connections]]
title = "main_pg"
type = "postgres"
connection = "postgres://pguser:password123@localhost:5432/outliers"
#connection = "$PG_CONSTR"
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

# Threshold Detector
# detection_method = 'threshold' 
# At least one of 'lower', 'upper' boundaries should be specified.
[[detectors]]
title = "Wiki Pageviews Threshold"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
backsteps = 90
cron_schedule = "*/5 * * * *"
notify_emails = [""]
detection_method = 'threshold'
lower = 220000000
upper = 300000000

# N Standard Deviations from the Mean Detector
# For each point takes points avg_window back,
# computes average and std. deviation.
# Mark the current point as an outlier if it is outside of average +- N * std.deviation interval
[[detectors]]
title = "Wiki Pageviews Stddev"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
backsteps = 90
cron_schedule = "*/5 * * * *"
notify_emails = [""]
detection_method = 'dist_from_mean'
avg_window = 30
sigma = 3

# Interquartile Range Detector
# For each point takes points iqr_window back,
# computes iqr_distance between q1=25 and q3=75 percentile.
# Mark the current point as an outlier if it is outside of
# q1 - iqr_range * iqr_distance, q3 + iqr_range * iqr_distance.
[[detectors]]
title = "Wiki Pageviews IQR"
connection = "main_pg"
data_sql = "select dt as t, views as value from wiki_pageviews where project='en.wikipedia'"
output = "wiki_pageviews_outliers"
backsteps = 90
cron_schedule = "*/5 * * * *"
notify_emails = [""]
detection_method = 'iqr'
iqr_window = 60
iqr_range = 1.5
```

uPlot: [https://github.com/leeoniya/uPlot](https://github.com/leeoniya/uPlot)
