# -*- coding: utf-8 -*-
import re
import sys
from typing import Callable
from urllib import parse, request

# import geopandas as gpd
import numpy as np
import pandas as pd
import pkg_resources

# Some display settings for numpy Array and pandas DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)

# Check if 'geopandas' can be loaded - if so, loaded, ignored otherwise
required = {'geopandas'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if not missing:
    import geopandas as gpd


def ts_step(
        ts: 'pd.DataFrame | pd.Series',
        minimum_time_step_in_second: int = 60
    ) -> 'int | None':
    """
    Identify the temporal resolution (in seconds) for a time series

    Parameters
    ----------
    ts : pd.DataFrame
        A Pandas DataFrame indexed by time/date.
    minimum_time_step_in_second : int, default=60
        The minimum threshold of the time step that can be identified.

    Raises
    ------
    TypeError
        When `not isinstance(ts.index, pd.DatetimeIndex)` is True.

    Returns
    -------
    int | None
        * **`-1`**: time series is not in a regular time step.
        * Any integer value **above `0`**: time series is regular (step in secs).
        * **`None`**: contains no values or a single value.
    """
    if not isinstance(ts.index, pd.DatetimeIndex):
        raise TypeError('`ts.index` must be `pd.DatetimeIndex`!')
    x = ts.dropna(axis=0, how='all')
    if x.shape[0] in (0, 1): return None
    diff_in_second = (pd.Series(x.index).diff() / np.timedelta64(1, 's')).values[1:]
    step_minimum = diff_in_second[diff_in_second >= minimum_time_step_in_second].min()
    return int(step_minimum) if (diff_in_second % step_minimum == 0).all() else -1


def na_ts_insert(ts: 'pd.DataFrame | pd.Series') -> pd.DataFrame:
    """
    Pad NaN value into a Timestamp-indexed DataFrame or Series

    Parameters
    ----------
    ts : pd.DataFrame | pd.Series
        A Pandas DataFrame or pd.Series indexed by time/date.

    Returns
    -------
    pd.DataFrame
        The NaN-padded Timestamp-indexed Series/DataFrame.

    Notes
    -----
        As for irregular time series, The empty-row-removed DataFrame returned.
    """
    r = pd.DataFrame(ts).dropna(axis=0, how='all')
    if r.shape[0] == 1: return r
    step = ts_step(ts)
    if (step == -1) or (step is None): return r
    r = r.asfreq(freq=f'{step}S')
    r.index.freq = None
    return r


def site_info(
        dataset: 'list[str] | str' = 'Global',
        measurementList: list[str] = None,
        spatial_ONLY: bool = False,
        office_use: bool = False
    ) -> 'gpd.GeoDataFrame | pd.DataFrame':
    """
    Obtain site information from Hilltop Server

    Parameters
    ----------
    dataset : str | list of str, default='Global'
        The datasets available from Hilltop Server.
    measurementList : str | list of str, default=None (all possible measurements)
        The requested measurements.
    spatial_ONLY : bool, default=False
        keep all (False, missing as `None`) or remove those (True) with no coordinates.
    office_use : bool, default=False
        Office access only (True).

    Returns
    -------
    gpd.GeoDataFrame | pd.DataFrame
        A gpd.GeoDataFrame of sites' info if geopandas installed, A pd.DataFrame otherwise.

    Example
    -------
    Retrieve all hydro site information (Global & Telemetry) from Hilltop Server

    >>> from urllib import parse, request
    >>> import re
    >>> import numpy as np
    >>> import pandas as pd
    >>> import geopandas as gpd

    >>> dataset = ['Global', 'Telemetry']
    >>> requested_M = [
            'Flow', 'Flow [Gauging Results]',
            'Rainfall', 'Daily Rainfall', 'VRainfall', 'Monthly Rainfall Total',
            'Water Temperature [Water Temperature]']
    >>> hydro_info = site_info(dataset, requested_M, office_use=True)

    Export the derived GeoDataFrame as an ESRI shapefile

    >>> hydro_info.to_file('hydro_info_global_telemetry_HS.shp', index=False)
    """
    if isinstance(dataset, str): dataset = [dataset]
    if isinstance(measurementList, str): measurementList = [measurementList]
    host = '192.168.1.195' if office_use else 'gisdata.orc.govt.nz'
    q_dict = {'Service': 'WFS', 'Request': 'GetFeature', 'TypeName': 'MeasurementList'}
    q_str = parse.urlencode(q_dict)
    R = None
    for hts in dataset:
        url = f'http://{host}/hilltop/{hts}.hts?{q_str}'
        html_str = request.urlopen(url).read().decode(encoding='utf-8').replace('\n', '')
        ML = re.findall(r'<MeasurementList>(.*?)</MeasurementList>', html_str)
        From = (re.findall(r'<From>(.*?)</From>', m)[0] for m in ML)
        To = (re.findall(r'<To>(.*?)</To>', m)[0] for m in ML)
        pos = [re.findall(r'<gml:pos>(.*?)</gml:pos>', m) for m in ML]
        tmp = pd.DataFrame(
            {
                'Site': (re.findall(r'<Site>(.*?)</Site>', m)[0] for m in ML),
                'dataset': hts,
                'Type': (re.findall(r'<Measurement>(.*?)</Measurement>', m)[0] for m in ML),
                'Lat': (float(l[0].split(' ')[0]) if l else np.nan for l in pos),
                'Long': (float(l[0].split(' ')[1]) if l else np.nan for l in pos),
                'From': pd.to_datetime(tuple(From), format='%Y-%m-%dT%H:%M:%S'),
                'To': pd.to_datetime(tuple(To), format='%Y-%m-%dT%H:%M:%S')
            }
        )
        R = pd.concat([R, tmp], axis=0, sort=False, ignore_index=True)
    if measurementList is not None:
        R = R.query('Type in @measurementList').reset_index(drop=True)
    R['Length_yr'] = (R['To'] - R['From']) / pd.Timedelta('365.2422D')
    R = R.astype({'From': str, 'To': str})
    R.loc[['gauging' in m for m in R['Type'].str.lower()], 'Length_yr'] = np.nan
    if 'geopandas' not in sys.modules:
        return R[['Site', 'dataset', 'Type', 'From', 'To', 'Length_yr', 'Long', 'Lat']]
    R['geometry'] = gpd.points_from_xy(R['Long'], R['Lat'], crs=4326)
    R = gpd.GeoDataFrame(R).to_crs(crs=2193).drop(columns=['Long', 'Lat'])
    R.loc[R.is_empty, 'geometry'] = None
    return R[R.is_valid].reset_index(drop=True) if spatial_ONLY else R


def _url_RFwT(
        *,
        office_use: bool,
        dataset: str,
        site: str,
        measurement: str,
        date_start: int = None,
        date_end: int = None,
    ) -> str:
    """
    URL of the requested rain/flow/water temperature from HilltopServer for a single site

    Parameters
    ----------
    office_use : bool
        Public access (False) or internal access (True).
    dataset : str
        A string of anyone in
        {'Global', 'Telemetry', 'WMGlobal', 'NIWAandGlobal', 'GlobalandWaterInfo'}
    site : str
        A string of a single site name.
    measurement : str
        A string the requested measurement.
    date_start : int | str, default=None - request the data from its very beginning)
        Start date of the requested data. It follows '%Y%m%d' if specified.
    date_end : int | str, default=None - request the data till its end (4 days from now on)
        End date of the request data date. It follows '%Y%m%d' if specified.

    Returns
    -------
    str
        The url of th requested time series
    """
    fmt = '%Y-%m-%dT%X'
    ds = '1890-01-01T00:00:00' if date_start is None else (
        pd.Timestamp(str(date_start))).strftime(format=fmt)
    end = (pd.Timestamp.now() + pd.Timedelta('4D')).strftime(format=fmt)
    de = end if date_end is None else (
        pd.Timestamp(str(date_end)) + pd.Timedelta('1D')).strftime(format=fmt)
    host = '192.168.1.195' if office_use else 'gisdata.orc.govt.nz'
    home = f'http://{host}/hilltop/{dataset}.hts'
    query_dict = {
        'Service': 'Hilltop',
        'Request': 'GetData',
        'Site': site.strip(),
        'Measurement': measurement,
        'TimeInterval': f'{ds}/{de}'
    }
    query_str = parse.urlencode(query_dict).replace('+', '%20')
    return f'{home}?{query_str}'


def RFwT(
        *,
        tidy: bool = False,
        raw: bool = False,
        **kw_url_RFwT
    ) -> pd.DataFrame:
    """
    Obtain Rain/Flow/WaterTemp (RFwT) time series from HilltopServer for a single site

    Parameters
    ----------
    tidy : bool, default=False
        Tidy version (`True`) of the data.
    raw : bool, default=False
        Whether get raw (True) time series (usually irrigular steps) or not (aggregated).
    **kw_url_RFwT : The arguments passed to `_url_RFwT` function

    Returns
    -------
    pd.DataFrame
        A DataFrame of a time series for a single site with a single measurement.

    """
    url = _url_RFwT(**kw_url_RFwT)
    site = kw_url_RFwT.pop('site').strip()
    measurement = kw_url_RFwT.pop('measurement')
    lines = [b.decode(encoding='utf-8').rstrip() for b in request.urlopen(url)]
    if len(lines) == 1:
        return (pd.DataFrame(columns=['Time', 'Site', 'Measurement', 'Value'])
            if tidy else pd.DataFrame(columns=['Time', site]))
    if raw:
        x = pd.DataFrame(
            {
                'Time': [re.findall(r'<T>(.*?)</T>', line) for line in lines],
                site: [re.findall(r'<I1>(.*?)</I1>', line) for line in lines],
            }
        ).applymap(lambda l: l[0] if l else np.nan)
        x[site] = x[site].astype(float)
        xx = x.dropna(subset=[site]).reset_index(drop=True)
    else:
        x = pd.DataFrame(
            {
                'Time': [re.findall(r'<T>(.*?)</T>', line) for line in lines],
                site: [re.findall(r'<I1>(.*?)</I1>', line) for line in lines],
                'Gap': [re.findall(r'<Gap/>', line) for line in lines]
            }
        ).applymap(lambda l: l[0] if l else np.nan)
        i_gap = x.query('Gap == Gap').index + 1
        x.loc[i_gap, site] = np.nan
        x[site] = x[site].astype(float)
        xx = x.dropna(subset=[site]).drop(columns=['Gap']).reset_index(drop=True)
    if tidy:
        xx.insert(1, 'Measurement', measurement)
        xx.insert(1, 'Site', site)
        xx = xx.rename(columns={site: 'Value'})
    return xx


def _HRain(**kw_RFwT) -> pd.DataFrame:
    """
    Get hourly rainfall time series ('VHourlyRainfall') from Hilltop Server
        'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site - see `help(RFwT)`
    """
    x = RFwT(measurement='VHourlyRainfall', tidy=False, raw=False, **kw_RFwT)
    if x.empty:
        print(f'[{x.columns[1]}] : NO rainfall data!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0)
    x['Time'] = pd.to_datetime(x['Time'].str[:13], format='%Y-%m-%dT%H')
    ts = x.set_index('Time').pipe(na_ts_insert)
    con = all(item in kw_RFwT.items() for item in {'date_start': None}.items())
    return ts if con else ts.iloc[1:, :]


def _DRain(**kw_RFwT) -> pd.DataFrame:
    """
    Get daily rainfall time series ('VDailyRainfall') from Hilltop Server
        'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site - see `help(RFwT)`
    """
    x = RFwT(measurement='VDailyRainfall', tidy=False, raw=False, **kw_RFwT).rename(
        columns={'Time': 'Date'})
    if x.empty:
        print(f'[{x.columns[1]}] : NO rainfall data!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0)
    x['Date'] = pd.to_datetime(x['Date'].str[:10], format='%Y-%m-%d') - pd.Timedelta('1D')
    ts = x.set_index('Date').pipe(na_ts_insert)
    con = all(item in kw_RFwT.items() for item in {'date_start': None}.items())
    return ts if con else ts.iloc[1:, :]


def _HFlo(**kw_RFwT) -> pd.DataFrame:
    """
    Get hourly flow time series ('Average Hourly Flow [Water Level]') from Hilltop Server
        'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site - see `help(RFwT)`
    """
    x = RFwT(measurement='Average Hourly Flow [Water Level]', tidy=False, raw=False,
        **kw_RFwT)
    if x.empty:
        print(f'[{x.columns[1]}] : NO flow data!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0)
    x['Time'] = pd.to_datetime(x['Time'].str[:13], format='%Y-%m-%dT%H')
    ts = x.set_index('Time').pipe(na_ts_insert)
    con = all(item in kw_RFwT.items() for item in {'date_start': None}.items())
    return ts if con else ts.iloc[1:, :]


def _DFlo(**kw_RFwT) -> pd.DataFrame:
    """
    Get daily flow time series ('Daily Flow [Water Level]') from Hilltop Server
        'Global|NIWAandGlobal|GlobalandWaterInfo' for a single site - see `help(RFwT)`
    """
    x = RFwT(measurement='Daily Flow [Water Level]', tidy=False, raw=False, **kw_RFwT)
    if x.empty:
        print(f'[{x.columns[1]}] : NO flow data!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0)
    x['Time'] = pd.to_datetime(x['Time'].str[:10], format='%Y-%m-%d') - pd.Timedelta('1D')
    ts = x.set_index('Time').rename_axis(index='Date').pipe(na_ts_insert)
    con = all(item in kw_RFwT.items() for item in {'date_start': None}.items())
    return ts if con else ts.iloc[1:, :]


def _HWU(**kw_RFwT) -> pd.DataFrame:
    """
    Get hourly WU rate time series ('WM Hourly Volume' in m³/s) from Hilltop Server
        'WMGlobal' for a single water meter - see `help(RFwT)`
    """
    x = RFwT(dataset='WMGlobal', measurement='WM Hourly Volume', tidy=False, raw=False,
        **kw_RFwT)
    if x.empty:
        x = RFwT(dataset='WMGlobal', measurement='Volume - Hourly [Flow]',
                 tidy=False, raw=False, **kw_RFwT)
    if x.empty:
        print(f'[{x.columns[1]}] : NO WU!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0) / 3600
    x['Time'] = pd.to_datetime(x['Time'].str[:13], format='%Y-%m-%dT%H')
    ts = x.set_index('Time').pipe(na_ts_insert)
    date_start = kw_RFwT.pop('date_start', None)
    return ts if date_start is None else ts.iloc[1:, :]


def _DWU(**kw_RFwT) -> pd.DataFrame:
    """
    Get daily WU rate time series ('WM Daily Volume' - m³/s) from Hilltop Server
        'WMGlobal' for a single water meter - see `help(RFwT)`
    """
    x = RFwT(dataset='WMGlobal', measurement='WM Daily Volume', tidy=False, raw=False,
        **kw_RFwT)
    if x.empty:
        x = RFwT(dataset='WMGlobal', measurement='Volume - Daily [Flow]',
                 tidy=False, raw=False, **kw_RFwT)
    if x.empty:
        print(f'[{x.columns[1]}] : NO WU!!\n')
    x.iloc[:, 1] = x.iloc[:, 1].mask(x.iloc[:, 1] < 0) / 86400
    x['Time'] = pd.to_datetime(x['Time'].str[:10], format='%Y-%m-%d')
    ts = x.set_index('Time').rename_axis(index='Date').pipe(na_ts_insert)
    date_start = kw_RFwT.pop('date_start', None)
    return ts if date_start is None else ts.iloc[:-1, :]


def _HD_HS(
        HDMY_func: Callable[..., pd.DataFrame],
        siteList: 'list[str] | str',
        tidy: bool = False,
        **kwargs
    ) -> pd.DataFrame:
    """
    Helper function for series of functions

    Parameters
    ----------
    HDMY_func : function
        The one from {_HRain, _DRain, _HFlo, _DFlo, _HWU, _DWU}.
    siteList : str, array-like
        A list of sites' names.
    tidy : bool, default=False
        Wide format (False) or long format output (True).
    **kwargs :
        Keyword arguments from the function defined in `HDMY_func`.
    """
    if isinstance(siteList, str): siteList = [siteList]
    ts = None
    for site in list(dict.fromkeys(siteList)):
        tmp = HDMY_func(site=site, **kwargs)
        ts = pd.concat([ts, tmp], axis=1, join='outer')
        ts.index.freq = None
    if tidy:
        dict_HDMY_func = {
            _HRain: 'Rainfall', _DRain: 'Rainfall',
            _HFlo: 'Flow', _DFlo: 'Flow',
            _HWU: 'WU_rate', _DWU: 'WU_rate'}
        value_name = dict_HDMY_func[HDMY_func]
        ts_melt = None
        for col in ts:
            ts_col_melt = na_ts_insert(ts[col]).reset_index().melt(
                id_vars=ts.index.name,
                value_vars=col,
                var_name='Site',
                value_name=value_name)
            ts_melt = pd.concat([ts_melt, ts_col_melt], axis=0)
        ts = ts_melt[['Site', ts.index.name, value_name]].reset_index(drop=True)
    return ts


def hourly_Rain_global(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly rainfall time series from 'Global' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Global', office_use=office_use)


def daily_Rain_global(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get daily rainfall time series from 'Global' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Global', office_use=office_use)


def hourly_Flo_global(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly flow time series from 'Global' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Global', office_use=office_use)


def daily_Flo_global(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get daily flow time series from 'Global' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Global', office_use=office_use)


def water_temp_global(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get water temerature time series (°C) in long format from 'Global' for multi sites.

    Parameters
    ----------
    siteList : array-like
        A list of sites' names.
    date_start : int | str, default=None (the very start of the requested data)
        Date start (either str in '%Y%m%d' or int YYYYmmdd) on request.
    date_end : int | str, default=None (the very end of the requested data)
        Date end (either str in '%Y%m%d' or int YYYYmmdd) on request.
    office_use : bool, default=True
        Public access (False) or internal access (True).

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame of water temperature time series.
    """
    if isinstance(siteList, str): siteList = [siteList]
    wtemp = None
    for site in list(dict.fromkeys(siteList)):
        tmp = RFwT(
            office_use=office_use,
            dataset='Global',
            site=site,
            measurement='Water Temperature',
            date_start=date_start,
            date_end=date_end,
            tidy=True,
            raw=True)
        if tmp.empty:
            print(f'[{site.strip()}] : NO water temperature data!!\n')
        wtemp = pd.concat([wtemp, tmp], axis=0, ignore_index=True, sort=False)
    wtemp['Time'] = pd.to_datetime(wtemp['Time'], format='%Y-%m-%dT%H:%M:%S')
    return wtemp


def hourly_Rain_telemetry(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly rainfall time series from 'Telemetry' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Telemetry', office_use=office_use)


def daily_Rain_telemetry(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get daily rainfall time series from 'Telemetry' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Telemetry', office_use=office_use)


def hourly_Flo_telemetry(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use=False
    ) -> pd.DataFrame:
    """
    Get hourly flow time series from 'Telemetry' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Telemetry', office_use=office_use)


def daily_Flo_telemetry(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get daily flow time series from 'Telemetry' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='Telemetry', office_use=office_use)


def water_temp_telemetry(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get water temerature time series (°C) in long format from 'Telemetry' for multi sites

    Parameters
    ----------
    siteList : array-like
        A list of sites' names.
    date_start : int | str, default=None (the very start of the requested data)
        Date start (either str in '%Y%m%d' or int YYYYmmdd) on request.
    date_end : int | str, default=None (the very end of the requested data)
        Date end (either str in '%Y%m%d' or int YYYYmmdd) on request.
    office_use : bool, default=True
        Public access (False) or internal access (True).

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame of water temperature time series.
    """
    if isinstance(siteList, str): siteList = [siteList]
    wtemp = None
    for site in list(dict.fromkeys(siteList)):
        tmp = RFwT(
            office_use=office_use,
            dataset='Telemetry',
            site=site,
            measurement='Water Temperature',
            date_start=date_start,
            date_end=date_end,
            tidy=True,
            raw=True)
        if tmp.empty:
            print(f'[{site.strip()}] : NO water temperature data!!\n')
        wtemp = pd.concat([wtemp, tmp], axis=0, ignore_index=True, sort=False)
    wtemp['Time'] = pd.to_datetime(wtemp['Time'], format='%Y-%m-%dT%H:%M:%S')
    return wtemp


def hourly_Rain_niwaGlobal(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly rainfall time series from 'NIWAandGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='NIWAandGlobal', office_use=True)


def daily_Rain_niwaGlobal(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get daily rainfall time series from 'NIWAandGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='NIWAandGlobal', office_use=True)


def hourly_Flo_niwaGlobal(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly flow time series from 'NIWAandGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='NIWAandGlobal', office_use=True)


def daily_Flo_niwaGlobal(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get daily flow time series from 'NIWAandGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='NIWAandGlobal', office_use=True)


def water_temp_niwaGlobal(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None
    ) -> pd.DataFrame:
    """
    Get water temerature time series (°C) from 'NIWAandGlobal' for multi sites

    Parameters
    ----------
    siteList : array-like
        A list of sites' names.
    date_start : int | str, default=None (the very start of the requested data)
        Date start (either str in '%Y%m%d' or int YYYYmmdd) on request.
    date_end : int | str, default=None (the very end of the requested data)
        Date end (either str in '%Y%m%d' or int YYYYmmdd) on request.

    Returns
    -------
    pd.DataFrame
        Water temerature time series (°C) in long format (tidy data).
    """
    if isinstance(siteList, str): siteList = [siteList]
    wtemp = None
    for site in list(dict.fromkeys(siteList)):
        tmp = RFwT(
            office_use=True,
            dataset='NIWAandGlobal',
            site=site,
            measurement='Water Temperature',
            date_start=date_start,
            date_end=date_end,
            tidy=True,
            raw=True)
        if tmp.empty:
            print(f'[{site.strip()}] : NO water temperature data!!\n')
        wtemp = pd.concat([wtemp, tmp], axis=0, ignore_index=True, sort=False)
    wtemp['Time'] = pd.to_datetime(wtemp['Time'], format='%Y-%m-%dT%H:%M:%S')
    return wtemp


def hourly_Rain_globalWaterInfo(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly rainfall time series from 'GlobalandWaterInfo' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='GlobalandWaterInfo', office_use=True)


def daily_Rain_globalWaterInfo(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get daily rainfall time series from 'GlobalandWaterInfo' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DRain, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='GlobalandWaterInfo', office_use=True)


def hourly_Flo_globalWaterInfo(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly flow time series from 'GlobalandWaterInfo' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='GlobalandWaterInfo', office_use=True)


def daily_Flo_globalWaterInfo(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False
    ) -> pd.DataFrame:
    """
    Get daily flow time series from 'GlobalandWaterInfo' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DFlo, siteList, tidy, date_start=date_start, date_end=date_end,
                  dataset='GlobalandWaterInfo', office_use=True)


def water_temp_globalWaterInfo(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None
    ) -> pd.DataFrame:
    """
    Get water temerature time series (°C) from 'GlobalandWaterInfo' for multi sites

    Parameters
    ----------
    siteList : array-like
        A list of sites' names.
    date_start : int | str, default=None (the very start of the requested data)
        Date start (either str in '%Y%m%d' or int YYYYmmdd) on request.
    date_end : int | str, default=None (the very end of the requested data)
        Date end (either str in '%Y%m%d' or int YYYYmmdd) on request.

    Returns
    -------
    pd.DataFrame
        Water temerature time series (°C) in long format (tidy data).
    """
    if isinstance(siteList, str): siteList = [siteList]
    wtemp = None
    for site in list(dict.fromkeys(siteList)):
        tmp = RFwT(
            office_use=True,
            dataset='GlobalandWaterInfo',
            site=site,
            measurement='Water Temperature',
            date_start=date_start,
            date_end=date_end,
            tidy=True,
            raw=True)
        if tmp.empty:
            print(f'[{site.strip()}] : NO water temperature data!!\n')
        wtemp = pd.concat([wtemp, tmp], axis=0, ignore_index=True, sort=False)
    wtemp['Time'] = pd.to_datetime(wtemp['Time'], format='%Y-%m-%dT%H:%M:%S')
    return wtemp


def hourly_WU(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get hourly WU rate time series from 'WMGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_HWU, siteList, tidy, date_start=date_start, date_end=date_end,
                  office_use=office_use)


def daily_WU(
        siteList: 'str | list[str]',
        date_start: 'int | str' = None,
        date_end: 'int | str' = None,
        tidy: bool = False,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Get daily WU rate time series from 'WMGlobal' for multi sites:
        `from fun import _HD_HS; help(_HD_HS)`
    """
    return _HD_HS(_DWU, siteList, tidy, date_start=date_start, date_end=date_end,
                  office_use=office_use)


def daily_WU_sim(
        df: pd.DataFrame,
        rm_rule: bool = None,
        sim: bool = True,
        office_use: bool = False
    ) -> pd.DataFrame:
    """
    Obtain all the available daily water use (WU) from Hilltop Server (WMGlobal)

    Parameters
    ----------
    df : pd.DataFrame
        A Pandas DataFrame of three columns:
            * 1st column - str, consent/consent group
            * 2nd column - str | list, water meters must be separated by ','
            * 3rd column - consented/maximum rate of take (in m³/s)
    rm_rule : float, default=None (for the raw daily)
        None for the raw daily; or scalar for times the consented to be removed.
    sim : bool, default=True
        If gaps filled process is needed (True).
    office_use : bool, default=False
        Public access (False) or internal use in the office (True).

    Returns
    -------
    pd.DataFrame
        Daily WU time series from Hilltop Server (WMGlobal), with gaps filled (`sim=True`).
    """
    df = df.rename(columns=dict(zip(df.columns, ['Consent', 'WM', 'Consented'])))
    df_data = df.loc[(df.loc[:, 'WM'] != '') & df.loc[:, 'WM'].notna(), :]
    wu = None
    for row in df_data.values:
        wm = row[1] if isinstance(row[1], list) else row[1].replace(' ', '').split(',')
        wu_wm = daily_WU(wm, office_use=office_use)
        if rm_rule is not None: wu_wm = wu_wm[wu_wm <= rm_rule * row[2]]
        if len(wm) > 1: wu_wm = wu_wm.sum(axis=1, min_count=1).to_frame()
        wu_wm.columns = [row[0]]
        wu = pd.concat([wu, wu_wm], axis=1, join='outer', sort=False)
    wu_arr = wu.values
    alloc_arr = np.repeat(df['Consented'].values.reshape((1, -1)), wu_arr.shape[0], axis=0)
    alloc_with_data_tmp = np.where(np.isfinite(wu_arr), alloc_arr, 0).sum(axis=1)
    alloc_with_data = np.where(alloc_with_data_tmp == 0, np.nan, alloc_with_data_tmp)
    ratio_alloc = np.nansum(wu_arr, axis=1) / alloc_with_data
    ratio = ratio_alloc.reshape((-1, 1))
    ratio_arr = np.repeat(ratio, wu.columns.size, axis=1)
    return pd.DataFrame(
        data=np.where(np.isfinite(wu_arr), wu_arr, alloc_arr*ratio_arr),
        index=wu.index, columns=wu.columns) if sim else wu


