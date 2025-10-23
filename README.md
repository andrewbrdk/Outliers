### Outliers

Timeseries outliers detection.  

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
