
library(data.table)  # Fast operations on large data frames
library(sf)  # Simple features, a standardized way to encode spatial vector data
library(stringi)  # A set of string processing tools


xts_2_dt <- function(xts_obj) {
  # Convert xts object to a data.table object
  res_dt <- as.data.table(xts_obj, keep.rownames = TRUE)
  colnames(res_dt)[1] <- if (!ts_step(res_dt)) "Date" else "Time"
  return(res_dt)
}


.as_df <- function(DF) {
  # Output the as data.frame related function based on the type (dt/df/tbl) of DF.
  if (data.table::is.data.table(DF) | xts::is.xts(DF)) {
    return(data.table::as.data.table)
  } else if (tibble::is_tibble(DF)) {
    return(tibble::as_tibble)
  } else {
    return(as.data.frame)
  }
}


.trim_ts <- function(TS) {
  # Remove the complete empty rows for a time series (TS) data.frame.
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   A time series (TS) data.frame with all completely empty rows removed.
  if (xts::is.xts(TS)) TS <- xts_2_dt(TS)
  n <- dim(TS)[2] - 1
  rm_L <- rowSums(is.na(TS[, -1, drop = FALSE])) == n
  ts_rmd <- if (length(which(rm_L))) TS[-which(rm_L), ] else TS
  res <- as.data.frame(ts_rmd)
  row.names(res) <- NULL
  return(.as_df(TS)(res))
}


ts_step <- function(TS, minimum_time_step_in_second = 60L) {
  # Identify the time step for a time series (can be multiple columns in a wide format)
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #   minimum_time_step_in_second: The threshold for checking time step, default is 60 secs.
  #
  # Returns:
  #   0: daily time step (days start from zero o'clock),
  #   -1: time series is not in a regular time step,
  #   any values greater than 0: time series is in a regular time step (in secs),
  #   NULL: time series doesn't contain any values (i.e., empty).
  t1 <- .trim_ts(TS)[[1]]
  if (any(c(0, 1) %in% length(t1)))
    return(cat("Not enough data to determine steps!"))
  if ("Date" %in% class(t1)) {
    return(0L)
  } else {
    t2 <- as.numeric(difftime(t1[-1], t1[-length(t1)], units = "secs"))
    t3 <- t2[t2 >= minimum_time_step_in_second]
    step_minimum <- min(t3)
    if (all(t3 %% step_minimum == 0)) return(step_minimum) else return(-1L)
  }
}


na_ts_insert <- function(TS) {
  # Padding the input time series (TS) by NA.
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   Padded time series for regular time-step TS;
  #   empty-row-removed time series for irregular time-step TS.
  TS <- .trim_ts(TS)
  ts_rmd <- as.data.frame(TS)
  if (dim(ts_rmd)[1]) {
    con <- ts_step(ts_rmd)
    rng <- range(ts_rmd[[1]], na.rm = TRUE)
    time_df <-
      if (con == 0) {
        data.frame(seq.Date(from = rng[1], to = rng[2], by = 1))
      } else if (con > 0) {
        data.frame(seq.POSIXt(from = rng[1], to = rng[2], by = con))
      } else {
        data.frame(ts_rmd[[1]])
      }
    names(time_df) <- names(TS)[1]
    res <- merge(time_df, ts_rmd, by = names(TS)[1], all.x = TRUE)
  } else {
    res <- ts_rmd
  }
  return(.as_df(TS)(res))
}


df_2_xts <- function(TS) {
  # Convert to an xts object.
  #
  # Args:
  #   TS: A data.frame (first column must be POSIXct or Date type).
  #
  # Returns:
  #   A xts object.
  if (is.data.table(TS)) return(as.xts.data.table(TS))
  if (is.data.frame(TS)) {
    ts_xts <- xts::xts(x = TS[, -1], order.by = TS[[1]])
    names(ts_xts) <- names(TS)[-1]
    return(ts_xts)
  }
}


site_info <- function(dataset = "Global", measurementList = NULL, spatial_ONLY = FALSE,
                      office_use = FALSE) {
  # Obtain site information (as a simple feature) from Hilltop Server.
  #
  # Args:
  #   dataset : char | list of char, optional
  #     The datasets available from Hilltop Server. The default is 'Global'.
  #   measurementList : char | list of char, optional
  #     The requested measurements. The default is NULL (all possible measurements).
  #   spatial_ONLY : logical, optional
  #     keep all (FALSE), remove those (TRUE) with no coordinates. The default is FALSE.
  #   office_use : logical, optional
  #     Office access only (TRUE). The default is FALSE
  #
  # Returns:
  #   sf - A simple feature of sites' information.
  host <- if (office_use) "192.168.1.195" else "gisdata.orc.govt.nz"
  R <- NULL
  for (hts in dataset) {
    URL <- paste0(
      "http://", host, "/hilltop/", hts, ".hts",
      "?Service=WFS&Request=GetFeature&TypeName=MeasurementList"
    )
    m <- fread(URL, sep = "\n", header = FALSE, col.names = "S", encoding = "UTF-8")
    f <- function(s, pattern, from, to)
      stri_sub(stri_extract_first(s, regex = pattern), from, to)
    m[, `:=`(
      Site = f(S, "<Site>(.*?)</Site>", 7L, -8L),
      Type = f(S, "<Measurement>(.*?)</Measurement>", 14L, -15L),
      From = f(S, "<From>(.*?)</From>", 7L, -8L),
      To = f(S, "<To>(.*?)</To>", 5L, -6L),
      pos = f(S, "<gml:pos>(.*?)</gml:pos>", 10L, -11L)
    )]
    i_rm <- m[is.na(Site) & is.na(Type) & is.na(From) & is.na(To) & is.na(pos), which = TRUE]
    r <- m[-i_rm, -"S"]
    r[, pos := shift(pos, -1)]
    loc <- na.omit(unique(r[, .(Site, pos)], by = "Site"))
    loc[, c("Lat", "Long") := as.data.table(stri_split_fixed(pos, " ", simplify = TRUE))]
    loc[, `:=`(pos = NULL, Lat = as.numeric(Lat), Long = as.numeric(Long))]
    r <- r[, .(
      Site = Site[which(!is.na(Site))],
      dataset = hts,
      Type = Type[which(!is.na(Type))],
      From = From[which(!is.na(From))],
      To = To[which(!is.na(To))]
    )]
    r[, `:=`(
      From = as.POSIXct(From, "%Y-%m-%dT%H:%M:%S", tz = "Etc/GMT-12"),
      To = as.POSIXct(To, "%Y-%m-%dT%H:%M:%S", tz = "Etc/GMT-12"),
      Length_yr = as.numeric(difftime(To, From, units = "days")) / 365.2422
    )]
    r[, `:=`(From = as.character(From), To = as.character(To))]
    tmp <- loc[r, on = "Site"]
    R <- rbind(R, tmp)
  }
  if (!is.null(measurementList)) R <- R[Type %in% measurementList]
  if (spatial_ONLY) R <- R[!is.na(Lat) & !is.na(Long)]
  R[which(grepl("gauging", R[, tolower(Type)])), Length_yr := NA]
  setDF(R)
  R_wgs84 <- st_as_sf(R, coords = c("Long", "Lat"), na.fail = spatial_ONLY, crs = 4326)
  return(st_transform(R_wgs84, crs = 2193))
}


YMD <- function(x) {
  # Easier way to have a "Date" vector
  #
  # Args:
  #   x: A vector of integers/characters following "%Y%m%d" format for dates.
  #
  # Returns:
  #   A vector of "Date" class.
  return(as.Date(as.character(x), format = "%Y%m%d"))
}


RFwT <- function(office_use, dataset, site, measurement, date_start = NA, date_end = NA,
                 tidy = FALSE, raw = FALSE) {
  # Obtain Rain/Flow/WaterTemp (RFwT) time series from HilltopServer for a single site.
  #
  # Args:
  #   office_use : bool. Public access (FALSE) or internal access (TRUE)
  #   dataset : char
  #     A string of anyone in
  #     c('Global', 'Telemetry', 'WMGlobal', 'NIWAandGlobal', 'GlobalandWaterInfo').
  #   site : char
  #     A string of a single site name.
  #   measurement : char
  #     The requested measurements. The default is NULL (all possible measurements).
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end : int, optional (default as NA)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end (4 days from current date on).
  #   tidy : bool, optional
  #     Long format (`TRUE`). Default is `FALSE`
  #   raw : bool, optional
  #     Raw time series (usually irregular time steps). Default is `FALSE`
  #
  # Returns:
  #   data.table
  fmt <- "%Y-%m-%dT00:00:00"
  t_s <- if (is.na(date_start)) "data start" else format(YMD(date_start), fmt)
  local_time <- Sys.time()
  attr(local_time, "tzone") <- "Etc/GMT-12"
  local_time_end <- format(local_time + 3600 * 24 * 5, format = fmt)
  t_e <- if (is.na(date_end)) local_time_end else format(YMD(date_end) + 1L, fmt)
  host <- if (office_use) "192.168.1.195" else "gisdata.orc.govt.nz"
  site <- stri_trim(site)
  URL <- paste0(
    "http://", host, "/hilltop/", dataset, ".hts",
    "?Service=Hilltop&Request=GetData&Site=", site,
    "&Measurement=", measurement,
    "&TimeInterval=", paste(t_s, t_e, sep = "/")
  )
  URL <- gsub(pattern = " ", replacement = "%20", x = URL)
  x <- fread(URL, sep = "\n", header = FALSE, col.names = "S", encoding = "UTF-8")
  if (x[, .N] == 1) {
    if (tidy) {
      res_dt <- data.table(
        Time = as.character(),
        Site = as.character(),
        Measurement = as.character(),
        Value = as.numeric()
      )
    } else {
      res_dt <- data.table(Time = as.character(), Site_name = as.numeric())
      setnames(res_dt, old = "Site_name", new = site)
    }
    return(res_dt[])
  }
  f <- function(s, pattern, from, to)
    stri_sub(stri_extract_first(s, regex = pattern), from, to)
  x[, `:=`(Time = f(S, "<T>(.*?)</T>", 4L, -5L), Value = f(S, "<I1>(.*?)</I1>", 5L, -6L))]
  if (!raw) {
    i_gap <- x[S == "<Gap/>", which = TRUE] + 1L
    x[i_gap, Value := NA]
  }
  x[, `:=`(Value = as.numeric(Value), S = NULL)]
  xx <- na.omit(x, cols = "Value")
  if (tidy) {
    xx[, `:=`(Site = site, Measurement = measurement)]
    setcolorder(xx, neworder = c("Time", "Site", "Measurement", "Value"))
  } else {
    setnames(xx, old = "Value", new = site)
  }
  return(xx[])
}


.HRain <- function(date_start = NA, ...) {
  # Get hourly rainfall time series ('VHourlyRainfall') from Hilltop Server
  #   'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(measurement = "VHourlyRainfall", date_start = date_start,
            tidy = FALSE, raw = FALSE, ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO rainfall data!!\n\n", sep = "")
  setnames(x, old = site, new = "Value")
  x[, Time := as.POSIXct(stri_sub(Time, 1L, 13L), "Etc/GMT-12", "%Y-%m-%dT%H")]
  x[Value < 0, Value := NA]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-1]))
}


.DRain <- function(date_start = NA, ...) {
  # Get daily rainfall time series ('VDailyRainfall') from Hilltop Server
  #   'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(measurement = "VDailyRainfall", date_start = date_start,
            tidy = FALSE, raw = FALSE, ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO rainfall data!!\n\n", sep = "")
  setnames(x, old = c("Time", site), new = c("Date", "Value"))
  x[, Date := as.Date(stri_sub(Date, 1L, 10L), format = "%Y-%m-%d", tz = "Etc/GMT-12") - 1L]
  x[Value < 0, Value := NA]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-1]))
}


.HFlo <- function(date_start = NA, ...) {
  # Get hourly flow time series ('Average Hourly Flow [Water Level]') from Hilltop Server
  #   'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(measurement = "Average Hourly Flow [Water Level]", date_start = date_start,
            tidy = FALSE, raw = FALSE, ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO flow data!!\n\n", sep = "")
  setnames(x, old = site, new = "Value")
  x[, Time := as.POSIXct(stri_sub(Time, 1L, 13L), "Etc/GMT-12", "%Y-%m-%dT%H")]
  x[Value < 0, Value := NA]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-1]))
}


.DFlo <- function(date_start = NA, ...) {
  # Get daily flow time series ('Daily Flow [Water Level]') from Hilltop Server
  #   'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(measurement = "Daily Flow [Water Level]", date_start = date_start,
            tidy = FALSE, raw = FALSE, ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO flow data!!\n\n", sep = "")
  setnames(x, old = c("Time", site), new = c("Date", "Value"))
  x[, Date := as.Date(stri_sub(Date, 1L, 10L), format = "%Y-%m-%d", tz = "Etc/GMT-12") - 1L]
  x[Value < 0, Value := NA]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-1]))
}


.HWU <- function(date_start = NA, ...) {
  # Get hourly water use time series ('WM Hourly Volume' - m³/s) from 'WMGlobal'
  #   for a single water meter.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(
    dataset = "WMGlobal",
    measurement = "WM Hourly Volume",
    date_start = date_start,
    tidy = FALSE,
    raw = FALSE,
    ...)
  if (!dim(x)[1])
    x <- RFwT(
      dataset = "WMGlobal",
      measurement = "Volume - Hourly [Flow]",
      date_start = date_start,
      tidy = FALSE,
      raw = FALSE,
      ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO WU!!\n\n", sep = "")
  setnames(x, old = site, new = "Value")
  x[, Time := as.POSIXct(stri_sub(Time, 1L, 13L), "Etc/GMT-12", "%Y-%m-%dT%H")]
  x[Value < 0, Value := NA]
  x[, Value := Value / 3600]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-1]))
}


.DWU <- function(date_start = NA, ...) {
  # Get daily water use time series ('WM Daily Volume' - m³/s) from 'WMGlobal'
  #   for a single water meter.
  #
  # Args:
  #   date_start : int, optional
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   ... : Arguments passed to function `RFwT()`.
  #
  # Returns:
  #   data.table
  x <- RFwT(
    dataset = "WMGlobal",
    measurement = "WM Daily Volume",
    date_start = date_start,
    tidy = FALSE,
    raw = FALSE,
    ...)
  if (!dim(x)[1])
    x <- RFwT(
      dataset = "WMGlobal",
      measurement = "Volume - Daily [Flow]",
      date_start = date_start,
      tidy = FALSE,
      raw = FALSE,
      ...)
  site <- names(x)[2]
  if (!x[, .N])
    cat("[", site, "] : ", "NO WU!!\n\n", sep = "")
  setnames(x, old = c("Time", site), new = c("Date", "Value"))
  x[, Date := as.Date(stri_sub(Date, 1L, 10L), format = "%Y-%m-%d", tz = "Etc/GMT-12")]
  x[Value < 0, Value := NA]
  x[, Value := Value / 86400]
  setnames(x, old = "Value", new = site)
  xx <- na_ts_insert(x)
  return(if (is.na(date_start)) xx else (if (!xx[, .N]) xx else xx[-.N]))
}


.HD_HS <- function(name_func, siteList, tidy = FALSE, ...) {
  # Helper function for series of functions.
  #
  # Args:
  #   name_func : char. ".HRain | .DRain | .HFlo | .DFlo | .HWU | .DWU"
  #   siteList : char. A list of sites' names
  #   tidy : logical. Wide format (FALSE, as default) or long format output (TRUE).
  #   ... : Arguments passed to function (string) defined in `name_func`.
  #
  # Returns:
  #   data.table
  ref_dt <- data.table(
    func = c(".HRain", ".DRain", ".HFlo", ".DFlo", ".HWU", ".DWU"),
    type_m = rep(c("Rainfall", "Flow", "WU_rate"), each = 2))
  f <- get(ref_dt[func == name_func, func])
  siteList <- unique(siteList)
  TS <- f(site = siteList[1], ...)
  if (length(siteList) > 1)
    for (site in siteList[-1]) {
      tmp <- f(site = site, ...)
      TS <- merge.data.table(TS, tmp, by = names(TS)[1], all = TRUE)
    }
  if (tidy) {
    Time <- names(TS)[1]
    Type <- ref_dt[func == name_func, type_m]
    TS_melt <- NULL
    for (site in siteList) {
      TS_site_melt <- melt.data.table(
        na_ts_insert(TS[, c(Time, site), with = FALSE]),
        id.vars = Time,
        variable.name = "Site",
        value.name = Type
      )
      TS_melt <- rbind(TS_melt, TS_site_melt)
    }
    setcolorder(TS_melt, neworder = c("Site", Time, Type))
    return(TS_melt[])
  }
  return(TS)
}


hourly_Rain_global <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                               office_use = FALSE) {
  # Get hourly rainfall time series from 'Global' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Global",
    office_use = office_use))
}


daily_Rain_global <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                              office_use = FALSE) {
  # Get daily rainfall time series from 'Global' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Global",
    office_use = office_use))
}


hourly_Flo_global <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                              office_use = FALSE) {
  # Get hourly flow time series from 'Global' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Global",
    office_use = office_use))
}


daily_Flo_global <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                             office_use = FALSE) {
  # Get daily flow time series from 'Global' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Global",
    office_use = office_use))
}


water_temp_global <- function(siteList, date_start = NA, date_end = NA, office_use = FALSE) {
  # Get raw water temperature from 'Global' for multiple sites, see `.HD_HS`.
  wtemp <- NULL
  for (site in unique(stri_trim(siteList))) {
    wt <- RFwT(office_use, "Global", site, "Water Temperature",
               date_start, date_end, TRUE, TRUE)
    wt[, Time := as.POSIXct(Time, "Etc/GMT-12", "%Y-%m-%dT%H:%M:%S")]
    if (!wt[, .N])
      cat("[", site, "] : ", "NO water temperature data!!\n\n", sep = "")
    wtemp <- rbind(wtemp, wt)
  }
  return(wtemp)
}


hourly_Rain_telemetry <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                                  office_use = FALSE) {
  # Get hourly rainfall time series from 'Telemetry' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Telemetry",
    office_use = office_use))
}


daily_Rain_telemetry <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                                 office_use = FALSE) {
  # Get daily rainfall time series from 'Telemetry' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Telemetry",
    office_use = office_use))
}


hourly_Flo_telemetry <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                                 office_use = FALSE) {
  # Get hourly flow time series from 'Telemetry' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Telemetry",
    office_use = office_use))
}


daily_Flo_telemetry <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                                office_use = FALSE) {
  # Get daily flow time series from 'Telemetry' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "Telemetry",
    office_use = office_use))
}


water_temp_telemetry <- function(siteList, date_start = NA, date_end = NA,
                                 office_use = FALSE) {
  # Get raw water temperature from 'Telemetry' for multiple sites, see `.HD_HS`.
  wtemp <- NULL
  for (site in unique(stri_trim(siteList))) {
    wt <- RFwT(office_use, "Telemetry", site, "Water Temperature",
               date_start, date_end, TRUE, TRUE)
    wt[, Time := as.POSIXct(Time, "Etc/GMT-12", "%Y-%m-%dT%H:%M:%S")]
    if (!wt[, .N])
      cat("[", site, "] : ", "NO water temperature data!!\n\n", sep = "")
    wtemp <- rbind(wtemp, wt)
  }
  return(wtemp)
}


hourly_Rain_niwaGlobal <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE) {
  # Get hourly rainfall time series from 'NIWAandGlobal' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "NIWAandGlobal",
    office_use = TRUE))
}


daily_Rain_niwaGlobal <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE) {
  # Get daily rainfall time series from 'NIWAandGlobal' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "NIWAandGlobal",
    office_use = TRUE))
}


hourly_Flo_niwaGlobal <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE) {
  # Get hourly flow time series from 'NIWAandGlobal' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "NIWAandGlobal",
    office_use = TRUE))
}


daily_Flo_niwaGlobal <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE) {
  # Get daily flow time series from 'NIWAandGlobal' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "NIWAandGlobal",
    office_use = TRUE))
}


water_temp_niwaGlobal <- function(siteList, date_start = NA, date_end = NA) {
  # Get raw water temperature from 'NIWAandGlobal' for multiple sites, see `.HD_HS`.
  wtemp <- NULL
  for (site in unique(stri_trim(siteList))) {
    wt <- RFwT(TRUE, "NIWAandGlobal", site, "Water Temperature",
               date_start, date_end, TRUE, TRUE)
    wt[, Time := as.POSIXct(Time, "Etc/GMT-12", "%Y-%m-%dT%H:%M:%S")]
    if (!wt[, .N])
      cat("[", site, "] : ", "NO water temperature data!!\n\n", sep = "")
    wtemp <- rbind(wtemp, wt)
  }
  return(wtemp)
}


hourly_Rain_globalWaterInfo <- function(siteList, date_start = NA, date_end = NA,
                                        tidy = FALSE) {
  # Get hourly rainfall from 'GlobalandWaterInfo' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "GlobalandWaterInfo",
    office_use = TRUE))
}


daily_Rain_globalWaterInfo <- function(siteList, date_start = NA, date_end = NA,
                                       tidy = FALSE) {
  # Get daily rainfall from 'GlobalandWaterInfo' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DRain",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "GlobalandWaterInfo",
    office_use = TRUE))
}


hourly_Flo_globalWaterInfo <- function(siteList, date_start = NA, date_end = NA,
                                       tidy = FALSE) {
  # Get hourly flow from 'GlobalandWaterInfo' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".HFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "GlobalandWaterInfo",
    office_use = TRUE))
}


daily_Flo_globalWaterInfo <- function(siteList, date_start = NA, date_end = NA,
                                      tidy = FALSE) {
  # Get daily flow from 'GlobalandWaterInfo' for multiple sites, see `.HD_HS`.
  return(.HD_HS(
    ".DFlo",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    dataset = "GlobalandWaterInfo",
    office_use = TRUE))
}


water_temp_globalWaterInfo <- function(siteList, date_start = NA, date_end = NA) {
  # Get raw water temperature from 'GlobalandWaterInfo' for multiple sites, see `.HD_HS`.
  wtemp <- NULL
  for (site in unique(stri_trim(siteList))) {
    wt <- RFwT(TRUE, "GlobalandWaterInfo", site, "Water Temperature",
               date_start, date_end, TRUE, TRUE)
    wt[, Time := as.POSIXct(Time, "Etc/GMT-12", "%Y-%m-%dT%H:%M:%S")]
    if (!wt[, .N])
      cat("[", site, "] : ", "NO water temperature data!!\n\n", sep = "")
    wtemp <- rbind(wtemp, wt)
  }
  return(wtemp)
}


hourly_WU <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                      office_use = FALSE) {
  # Get hourly WU rate time series from 'WMGlobal' for multiple sites.
  p <- "^[wW][mM]\\d{4}\\w?$|^[dD][sS]\\d{4}\\w?$"
  for (s in siteList) {
    if (!stri_detect(s, regex = p))
      cat(paste0("[", s, "] is NOT a valid water meter - Ignored!!\n\n"))
  }
  siteList <- siteList[stri_detect(siteList, regex = p)]
  if (!length(siteList))
    return(cat("Please use the valid water meters!!\n\n"))
  return(.HD_HS(
    ".HWU",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    office_use = office_use))
}


daily_WU <- function(siteList, date_start = NA, date_end = NA, tidy = FALSE,
                     office_use = FALSE) {
  # Get daily WU rate time series from 'WMGlobal' for multiple sites.
  p <- "^[wW][mM]\\d{4}\\w?$|^[dD][sS]\\d{4}\\w?$"
  for (s in siteList) {
    if (!stri_detect(s, regex = p))
      cat(paste0("[", s, "] is NOT a valid water meter - Ignored!!\n\n"))
  }
  siteList <- siteList[stri_detect(siteList, regex = p)]
  if (!length(siteList))
    return(cat("Please use the valid water meters!!\n\n"))
  return(.HD_HS(
    ".DWU",
    siteList,
    tidy,
    date_start = date_start,
    date_end = date_end,
    office_use = office_use))
}


rowsum_df <- function(DF, na.rm = FALSE, min_count = NULL) {
  # Calculate the row sum for a data.frame
  #
  # Args:
  #   DF: A numeric data.frame with more than one columns.
  #   na.rm: logical. Should missing values (including NA) be omitted from the calculations?
  #   min_count: The required number of valid values to perform the operation.
  #              If fewer than ``min_count`` non-NA values are present the result will be NA.
  #
  # Returns:
  #   A vector containing the sums for each row.
  if (is.null(min_count)) {
    v <- rowSums(DF, na.rm = na.rm)
  } else {
    row_count_not_na <- rowSums(!is.na(DF))
    v <- rowSums(DF, na.rm = TRUE)
    v[which(row_count_not_na < min_count)] <- NA
  }
  return(v)
}


daily_WU_sim <- function(DF, rm_rule = NULL, sim = TRUE, office_use = FALSE) {
  # Obtain all the available daily water use (WU) from Hilltop Server (WMGlobal)
  #
  # Args:
  #   DF : data.frame
  #     A data.frame of three columns:
  #       * 1st column - str, consent/consent group
  #       * 2nd column - str | list, water meters must be separated by ','
  #       * 3rd column - consented rate of take
  #   rm_rule : bool, optional
  #     None for the raw daily; or scalar for times the consented to be removed.
  #     The default is NULL
  #   sim : bool, optional
  #     If gaps filled process is needed (TRUE). The default is TRUE
  #   office_use : bool. Public access (FALSE) or internal access (TRUE)
  #
  # Returns:
  #   data.table
  x <- as.data.frame(DF)
  names(x) <- c("Consent", "WM", "Allocation")
  df_data <- x[which(!is.na(x$WM) & x$WM != ""), ]
  wu <- data.frame(Date = as.Date(NA))
  for (i in 1:dim(df_data)[1]) {
    wm <- strsplit(gsub(" ", "", df_data[i, 2]), split = ",", fixed = TRUE)[[1]]
    wu.wm <- daily_WU(wm, office_use = office_use)
    if (!is.null(rm_rule))
      wu.wm[, -1][wu.wm[, -1, drop = FALSE] > rm_rule * df_data[[i, 3]]] <- NA
    if (length(wm) > 1)
      wu.wm <- data.frame(
        Date = wu.wm$Date,
        Value = rowsum_df(wu.wm[, -1], min_count = 1))
    names(wu.wm)[2] <- df_data[[i, 1]]
    wu <- merge(wu, wu.wm, by = "Date", all = TRUE)
  }
  wu_tmp <- wu <- na_ts_insert(wu)
  wu_tmp[setdiff(x$Consent, df_data$Consent)] <- NA
  wu_tmp <- wu_tmp[, c("Date", x$Consent)]
  wu_mx <- data.matrix(wu_tmp[, -1, drop = FALSE])
  alloc_mx <- matrix(
    rep(t(x$Allocation), times = dim(wu_tmp)[1]),
    nrow = dim(wu_tmp)[1],
    byrow = TRUE)
  alloc_with_data_tmp <- rowSums(ifelse(is.na(wu_mx), 0, alloc_mx))
  alloc_with_data <- ifelse(is.na(alloc_with_data_tmp), NA, alloc_with_data_tmp)
  ratio <- rowSums(wu[, -1, drop = FALSE], na.rm = TRUE) / alloc_with_data
  ratio_mx <- matrix(rep(t(ratio), times = length(x$Consent)), ncol = length(x$Consent))
  wu_sim <- data.frame(wu_tmp$Date, ifelse(is.na(wu_mx), alloc_mx * ratio_mx, wu_mx))
  names(wu_sim) <- names(wu_tmp)
  return(.as_df(DF)(if (sim) wu_sim else wu))
}


