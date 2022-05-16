import logging
from typing import Dict

import numpy as np
import pandas as pd


def add_bmi(df: pd.DataFrame, column_height_units: str = None, column_weight_units: str = None,
            column_height: str = 'HEIGHT', column_weight: str = 'WEIGHT', column_bmi: str = 'BMI') -> pd.DataFrame:
    """
    Calculates bmi in US and metric units, value by value. Firstly converts string value to float, if couldn't,
    throws an exception.
    """

    def bmi(row: pd.Series) -> pd.Series:
        weight = row[column_weight]
        height = row[column_height]
        if column_height_units:
            heightu = row[column_height_units].lower()
        else:
            heightu = "cm"

        if column_weight_units:
            weightu = row[column_weight_units].lower()
        else:
            weightu = "kg"
        try:
            if weight != '' and weight is not None and height != '' and height is not None:
                weight = float(weight)
                height = float(height)
                if heightu == "in":
                    height *= 2.54
                elif heightu == "m":
                    height *= 100

                if weightu == "lb":
                    weight *= 0.453592
                row[column_bmi] = str(round(weight / (0.01 * height) ** 2, 1))
            else:
                row[column_bmi] = ""

        except ValueError:
            logging.error('Wrong weight or height format')
            row[column_bmi] = ""
        return row

    df = df.apply(bmi, axis=1, result_type='reduce')
    return df


def add_ongo(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Adds column with Y/N values if study still running. Throws an exception if couldn't find end date of the study.
    """
    column_endtc = prefix + "ENDTC"
    column_ongo = prefix + "ONGO"
    try:
        df[column_ongo] = df[column_endtc].apply(lambda x: "Y" if x in ["", None] else "N")
    except Exception:
        logging.error('Probably column endtc does not exist')
    return df


def add_race(df: pd.DataFrame, races: Dict[str, str]) -> pd.DataFrame:
    """
    Adds column with race instead of dummy columns, uses dictionary to replace values. If there exists more than one 1,
    sets MULTIPLE as a race.
    """

    def get_race(row: pd.Series) -> str:
        value = ''
        for column, race_name in races.items():
            if column in row.index:
                if row[column] not in ['', '0', '0.0', 0, 'nan']:
                    if value == '':
                        value = race_name
                    else:
                        value = 'MULTIPLE'
        if value == '':
            value = np.nan
        return value

    df['RACE'] = df.apply(get_race, axis=1, result_type='reduce')
    return df
