import numpy as np
import pandas as pd
import typing
from datetime import date

import sdw4_mapping
import sdw4_mapping.transformations
import sdw4_mapping.transformations.preparation
import sdw4_mapping.transformations.date_conversions as sdw_dt


print("Applying patches!!!!!!!!!")


# Patch convert_to_dt() to handle NaN"

_convert_to_dt_original = sdw_dt.convert_to_dt


def _convert_to_dt_patched(dt, start = True):
    try:
        if pd.isnull(dt):
            return dt
        else:
            return _convert_to_dt_original(dt, start)
    except Exception as e:
        print("_convert_to_date_patched" +
                f"({dt}:{type(dt)},{start}:{type(start)}):{e}")
        return np.nan

sdw_dt.convert_to_dt = _convert_to_dt_patched



# Have add_duration() deal with pd.NaT using pd.notnull

def _add_duration(df: pd.DataFrame, column_start: str, column_end: str, column_dur: str = 'DURATION',
                 ) -> pd.DataFrame:
    """
    Calculates study duration. Firstly trying to convert birth date from string to date using "date format" parameter.
    Throws an exception every time when couldn't convert and returns None.
    """

    def calculate_dur(row: pd.DataFrame) -> str:
        start = sdw_dt.convert_to_dt(row[column_start])
        end = sdw_dt.convert_to_dt(row[column_end])

        if (isinstance(start, date) and pd.notnull(start)
                and isinstance(end, date) and pd.notnull(end)):
            delta = str((end - start).days)
        else:
            delta = None
        return delta

    df[column_dur] = df.apply(calculate_dur, axis=1, result_type="reduce")
    return df

sdw_dt.add_duration = _add_duration



# Empty domains sometimes get type error.
_filter_rows_original = sdw4_mapping.transformations.preparation.filter_rows
def _filter_rows(df: pd.DataFrame, 
                        filter_function: typing.Callable,
                        column: str = None):
    if len(df) == 0:
        return df.copy()
    else:
        return _filter_rows_original(df, filter_function, column)
sdw4_mapping.transformations.preparation.filter_rows = _filter_rows

