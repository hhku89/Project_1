import logging
import re
from calendar import monthrange
from datetime import date, datetime
from typing import Optional, Union
import dateparser
import pandas as pd


def convert_to_dt(dt: Union[str, date, None], start: bool = True) -> Optional[str]:
    """
    Converts single string to datetime using dateparser library
    https://dateparser.readthedocs.io/en/latest/settings.html).
    Before dataparser checks for patterns using _check_regexp method.
    Cuts timezone and microseconds if exist.
    Some invalid parsing will be set as None.
    """
    converted_dt = dt
    if dt is not None and not isinstance(dt, date):
        dt = _check_regexp(dt, start=start)
        if not isinstance(dt, date) and dt is not None:
            try:
                converted_dt = dateparser.parse(dt, settings={'PREFER_DAY_OF_MONTH': 'first' if start else 'last',
                                                              'PREFER_DATES_FROM': 'past'})
                if converted_dt is None:
                    logging.debug(f'Wrong date format: {dt}')
                    return dt
                else:
                    if converted_dt.microsecond != 0:
                        converted_dt = converted_dt.replace(microsecond=0).replace(tzinfo=None)
            except ValueError:
                logging.debug(f'Wrong date format: {dt}')
    return converted_dt


def convert_date_smart(df: pd.DataFrame, date_column: str, only_date: bool = False, start: bool = None) -> pd.DataFrame:
    """
    Tries to identify date format automatically and convert column values.
    Strips date string before parsing. Сurrent implementation gets substring from column name and decides if the end of
    period (year or month) will be used to fill gaps in partial date, else - the beginning of the period. Also user can
    set this parameter "start" by himself to True or False.
    Firstly uses pandas.to_datetime function, if even one values from column wasn't converted successfully, raises an exception.
    If an exceptions raised, tries to convert to datetime format with  using convert_to_dt method.
    But it is much slower!!! Because dateparser parses date by date.
    By setting only_date to True, can cut converted datetimes to dates.
    Returns dataframe with converted values in the column.
    """
    try:
        if start is None:
            start = False if date_column.endswith("ENDTC") or date_column.endswith("ENDAT") else True

        if pd.api.types.is_string_dtype(df[date_column]):
            df[date_column] = df[date_column].str.strip()
        try:
            df[date_column] = pd.to_datetime(df[date_column], errors='raise', infer_datetime_format=True)
        except:
            logging.debug("Couldn't parse date column by pandas.to_datetime, trying to parse date by date...")
            df[date_column] = df[date_column].apply(convert_to_dt, args=(start,))

        if only_date:
            df[date_column] = df[date_column].dt.date
    except Exception as e:
        if date_column not in df.columns:
            logging.error(f'There is no such column {date_column} in the data!')
        else:
            logging.error(e)
    return df


def convert_date_format(df: pd.DataFrame, date_column: str,
                        date_format: Optional[str] = None, only_date=False) -> pd.DataFrame:
    """
    Converts date column values using pandas.to_datetime with date format that user gave. If date_format is None,
    pandas tries to guess it.
    """
    if pd.api.types.is_string_dtype(df[date_column]):
        df[date_column] = df[date_column].str.strip()
    try:
        df[date_column] = pd.to_datetime(df[date_column], format=date_format, errors='ignore')
    except Exception as e:
        logging.error(f'Format {date_format} is wrong, {e}')
        return df

    if only_date:
        df[date_column] = df[date_column].dt.date

    if pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = df[date_column].apply(lambda x: x.replace(microsecond=0).replace(tzinfo=None))
    return df


def add_age_from_birth_year(df: pd.DataFrame, column_birth_year: str, column_age: str = "AGE") -> pd.DataFrame:
    """
    Calculates age as delta from birth year and current year. If year can't be converted from string to int format,
    throws an exception.
    """
    now = datetime.now()

    def get_age(birth_year: str):
        try:
            if birth_year is not None:
                return str(now.year - int(birth_year))
            else:
                return None
        except Exception:
            logging.error('Wrong year format')
            return None

    df[column_age] = df[column_birth_year].apply(get_age)
    return df


def add_age_from_birth_date(df: pd.DataFrame, column_birth_date: str, column_age: str = "AGE",
                            ) -> pd.DataFrame:
    """
    Calculates age as delta from birth date and current date. Firstly trying to convert birth date from string to date.
    If unsuccessfully, throws an exception.
    """
    today = date.today()

    def get_age(born: str):
        try:
            if born is not None:
                born = convert_to_dt(born)
                return str(today.year - born.year - ((today.month, today.day) < (born.month, born.day)))
        except Exception:
            logging.error('Wrong date format')
        return None

    df[column_age] = df[column_birth_date].apply(get_age)
    return df


def add_duration(df: pd.DataFrame, column_start: str, column_end: str, column_dur: str = 'DURATION',
                 ) -> pd.DataFrame:
    """
    Calculates study duration. Firstly trying to convert birth date from string to date using "date format" parameter.
    Throws an exception every time when couldn't convert and returns None.
    """

    def calculate_dur(row: pd.DataFrame) -> str:
        start = convert_to_dt(row[column_start])
        end = convert_to_dt(row[column_end])

        if isinstance(start, date) and isinstance(end, date):
            delta = str((end - start).days)
        else:
            delta = None
        return delta

    df[column_dur] = df.apply(calculate_dur, axis=1, result_type="reduce")
    return df


def add_dy(df, dtc_column, dy_column=None):
    """
    Calculates a study day variable like *Domain*STUDY, which is the difference between RFSTDTC and *Domain*STDTC.
    """
    if not dtc_column.endswith("DTC") and dy_column is None:
        error_msg = f'Invalid column name {dtc_column} (must end with "DTC", or specify dy_column)'
        logging.error(error_msg)
        return df

    # create dy_column name if wasn't specified
    dy_column = dy_column if dy_column is not None else dtc_column.replace("DTC", "DY")

    def calculate_study_day(row: pd.DataFrame) -> str:
        try:
            dtc = pd.Timestamp(row[dtc_column])
            if not isinstance(dtc, date):
                dtc = (dtc)
            rfstdtc = convert_to_dt(row['RFSTDTC'])
            delta = dtc - rfstdtc
            dy = delta.days
            if dy >= 0:
                dy += 1
        except Exception:
            logging.error('Calculation of study day went wrong: ' + str(row[dtc_column]))
            dy = None
        return str(dy)

    df[dy_column] = df.apply(calculate_study_day, axis=1, result_type="reduce")
    return df


def _combine_partial_date(orig_date: str, year: str = None, month: str = None, day: str = None,
                          start: bool = False, hour: str = '00', minute: str = '00', second: str = '00'):
    """
    If date is partial, imputes missing parts depending on the "start" parameter.
    Combines all parts to date string and returns.
    """
    log_msg_args = ''
    if year is None:
        year = datetime.now().year
        log_msg_args = log_msg_args + 'year, '
    if month is None:
        month = 1 if start else 12
        log_msg_args = log_msg_args + 'month, '
    else:
        try:
            month = int(month)
        except Exception:
            month = datetime.strptime(month, "%b").month
    if month is not None and day is None:
        log_msg_args = log_msg_args + 'day'
        _, end_of_month = monthrange(year=int(year), month=month)
        day = 1 if start else end_of_month
    dtc = str(year) + '-' + str(month) + '-' + str(
        day) + ' ' + hour + ':' + minute + ':' + second
    if log_msg_args != '':
        logging.debug(f'Missing {log_msg_args} in {orig_date}')
    return dtc


def _check_regexp(dtc: str, start: bool = False):
    """
    Checks date string for matching particular format.
    If matches, extract existing parts and pass to the _combine_partial_date method to impute missing parts.
    Returns date string after combining using latter.
    """
    if re.match(r"UN UNK \d\d\d\d*$", dtc):
        year = dtc[-4:]
        dtc = _combine_partial_date(year=year, start=start, orig_date=dtc)
    elif re.match(r"\d{2}\w{3}\d{4}:\d{2}:\d{2}:\d{2}.\d{3}", dtc):
        year = dtc[5:9]
        day = dtc[:2]
        month = dtc[2:5]
        hours = dtc[10:12]
        minutes = dtc[13:15]
        seconds = dtc[16:18]

        dtc = _combine_partial_date(year=year, month=month,
                                    day=day, hour=hours,
                                    minute=minutes, second=seconds, start=start, orig_date=dtc)
    elif re.match(r"\d{2}---\d{4}", dtc) or re.match(r"\d{2}[-/]---[-/]\d{4}", dtc):
        day = dtc[:2]
        year = dtc[-4:]
        dtc = _combine_partial_date(year=year,
                                    day=day, start=start, orig_date=dtc)
    elif re.match(r"\d{4}-----", dtc) or re.match(r"\d{4}[-/]---[-/]--", dtc):
        year = dtc[:4]
        dtc = _combine_partial_date(year=year, start=start, orig_date=dtc)
    elif re.match(r"\d{2}[-/]\w{3}[-/]----", dtc):
        day = dtc[:2]
        month = dtc[3:6]
        dtc = _combine_partial_date(start=start, orig_date=dtc, day=day, month=month)
    elif re.match(r"UN[-/\s+]\w{3}[-/\s+]\d{2}", dtc):
        year = dtc[-4:]
        month = dtc[3:6]
        dtc = _combine_partial_date(year=year, month=month, start=start, orig_date=dtc)
    elif len(dtc) == 4:
        year = dtc
        dtc = _combine_partial_date(year=year, start=start, orig_date=dtc)
    return dtc
