# HilltopServer helper functions (Otago, New Zealand)

Tools that help to retrieve time series data from public Hilltop Server for Otago (New Zealand).

Otago Regional Council (ORC) provides the public access to the Hilltop Server for Global and Telemetry. Python/R functions listed below provides an easier access to the flow/rainfall/water use time series from the public Hilltop Server **Global**, **Telemetry**, and **GlobalWM**.

* **Global**: This dataset contains all the possible certified hydro-related data till April 2021 from Otago Regional Council. The functions are:
    * `hourly_Rain_global`
    * `daily_Rain_global`
    * `hourly_Flo_global`
    * `daily_Flo_global`
    * `water_temp_global`
* **Telemetry**: This dataset contains all the latest records from ORC's telemetry sites, but not certified. They are:
    * `hourly_Rain_telemetry`
    * `daily_Rain_telemetry`
    * `hourly_Flo_telemetry`
    * `daily_Flo_telemetry`
    * `water_temp_telemetry`
* **GlobalWM**: This dataset has all possible water use data (water meters as the site names) till the latest. They are:
    * `hourly_WU`
    * `daily_WU`

The next section presents several examples in details on how to obtain time series (either complete or for a period) from the mentioned public Hilltop Server.

## Examples (python)

Required modules:

* [`geopandas`](https://geopandas.org/en/stable) (optional)
* [`numpy`](https://numpy.org)
* [`pandas`](https://pandas.pydata.org)

### 1. Daily flow (m<sup>3</sup>/s) time series retrieval

```py
from HilltopServer import daily_Flo_global, daily_Flo_telemetry

# Get all daily flow from Hilltop Global for the following site
site = 'Arrow at Cornwall street d/s'
x_global = daily_Flo_global(site)

# Get the latest flow from Hilltop Telemetry for the two sites
x_telemetry = daily_Flo_telemetry(site)

# Use the telemetry flow data to update those obtained from Global to make a complete dataset
x = x_global.combine_first(x_telemetry)
```

### 2. Hourly rainfall (mm) time series retrieval

```py
from HilltopServer import hourly_Rain_global, hourly_Rain_telemetry

# Get all the available hourly rainfall data for site
rain_gauge = ['Dart at Paradise', 'Dart at The Hillocks']

# Get the rainfall data from Hilltop Global
y_global = hourly_Rain_global(rain_gauge)

# Get the rainfall data from Hilltop Telemetry
y_telemetry = hourly_Rain_telemetry(rain_gauge)
 
# Use the Telemetry rainfall data to update those obtained from Global to make a complete dataset
y = y_global.combine_first(y_telemetry)
```

### 3. All daily water use (m<sup>3</sup>/s) time series retrieval

```py
from HilltopServer import daily_WU

# Get daily water use (WU) between 2014-06-26 and 2017-12-14 (in tidy format)
WM = ['WM0062', 'WM0906']
z = daily_WU(WM, date_start=20140626, date_end=20171214, melt=True)
```

## Examples (R)

Required libraries:

* [`data.table`](https://cran.r-project.org/web/packages/data.table)
* [`sf`](https://cran.r-project.org/web/packages/sf)
* [`stringi`](https://cran.r-project.org/web/packages/stringi)

### 1. Daily flow (m<sup>3</sup>/s) time series retrieval

```r
source("HilltopServer.R")

# Get all daily flow from Hilltop Global for the following site
site <- 'Arrow at Cornwall street d/s'
x_global <- daily_Flo_global(site)

# Get the latest flow from Hilltop Telemetry for the two sites
x_telemetry <- daily_Flo_telemetry(site)

# Use the telemetry flow data to update those obtained from Global to make a complete dataset
tmp_name <- paste0("telemetry_", site)
setnames(x_telemetry, old = site, new = tmp_name)
x <- merge.data.table(x_global, x_telemetry, by = "Date", all = TRUE)
x[, c(site, tmp_name) := .(fifelse(is.na(get(site)), get(tmp_name), get(site)), NULL)]
```

### 2. Hourly rainfall (mm) time series retrieval

```r
source("HilltopServer.R")

# Get all the available hourly rainfall data for site
rain_gauge <- c("Dart at Paradise", "Dart at The Hillocks")

# Get the rainfall data from Hilltop Global
y_global <- hourly_Rain_global(rain_gauge)

# Get the rainfall data from Hilltop Telemetry
y_telemetry <- hourly_Rain_telemetry(rain_gauge)
 
# Use the Telemetry rainfall data to update those obtained from Global to make a complete dataset
tmp_name <- paste0("telemetry_", rain_gauge)
setnames(y_telemetry, old = rain_gauge, new = tmp_name)
y <- merge.data.table(y_global, y_telemetry, by = "Time", all = TRUE)
for (i in rain_gauge) {
  i_tmp <- paste0("telemetry_", i)
  y[, c(i, i_tmp) := .(fifelse(is.na(get(i)), get(i_tmp), get(i)), NULL)]
}
```

### 3. All daily water use (m<sup>3</sup>/s) time series retrieval

```r
source("HilltopServer.R")

# Get daily water use (WU) between 2014-06-26 and 2017-12-14 (in tidy format)
WM <- c("WM0062", "WM0906")
z <- daily_WU(WM, date_start = 20140626, date_end = 20171214, melt = TRUE)
```

