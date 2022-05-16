import logging
from typing import Dict, Any, List, Callable, Optional, Union, TypedDict

import pandas as pd


class TUnpivotAttr(TypedDict):
    val: Optional[str]
    col: Optional[str]


class TUnpivotColumn(TypedDict):
    key: Optional[str]
    attr: Optional[TUnpivotAttr]


TUnpivotColumnConfig = Union[TUnpivotColumn, List[Union[None, str, List[str]]]]


def rename_columns(df: pd.DataFrame, columns: Union[Dict[str, str], dict], inplace: bool = True) -> pd.DataFrame:
    """
    Renames columns by names as a dictionary e.g. {"A": "a", "B": "c"}.
    By default inplace is True, so it removes columns if names are duplicated.
    If inplace is False, leaves columns as it is and warns user.

    """
    if len(columns) == 0:
        return df
    non_identical_columns = {key: val for key, val in columns.items() if key != val}
    columns_to_drop = list(set(non_identical_columns.values()).intersection(set(df.columns)))
    if len(columns_to_drop) > 0:
        if not inplace:
            logging.error(
                f"Won't do the rename. {columns_to_drop} columns are clashing during the column rename: {str(columns)}. ")
            return df

        df = drop_columns(df, columns=columns_to_drop)
        logging.warning(f"{columns_to_drop} columns were replaced by column rename: {str(columns)}.")

    return df.rename(columns=columns)


def add_column_constant(df: pd.DataFrame, column: str, value: Any, inplace=True) -> pd.DataFrame:
    """
    Adds column with constant value.
    If column name already exists, can be inplaced by setting inplace to True (True is default).
    If inplace is False, adds to duplicated name '_1'
    """
    if column in df.columns:
        if inplace:
            df = drop_columns(df, columns=[column])
            logging.warning(f"{column} column was replaced by column creation with the value '{str(value)}'.")
        else:
            column = column + '_1'
    df[column] = value
    return df


def map_column(df: pd.DataFrame, source_column: str, dictionary: Dict[Union[str, None], Union[str, None]],
               output_column_name: str = None, default_value: Any = False) -> pd.DataFrame:
    """
    Replaces values in the column with values from a dictionary.
    Values in the dictionary should be the same type as in the dataframe. E.g. if user wants to replace 'a' with 'b' and
    1 with 2, the dictionary should look like {'a': 'b', 1: 2}
    To replace values that weren't found in the dictionary, set default value. Otherwise all that wasn't would be changed to None.
    If you want to replace cells that weren't found, set default_value to anything except False.

    """
    if output_column_name is None:
        output_column_name = source_column

    pre_values = df[source_column].copy()
    if default_value is not False:
        df[output_column_name] = df[source_column].map(dictionary)
        if default_value is not None:
            df[output_column_name] = df[source_column].fillna(default_value)
    else:
        try:
            df[output_column_name] = df[source_column].replace(dictionary)
        except Exception:
            logging.error("Couldn't replace values")
    if pre_values.equals(df[source_column]):
        logging.error('No values have been replaced!')

    df[output_column_name] = _replace_nan_to_None(df[output_column_name])
    return df


def copy_columns(df: pd.DataFrame, source_columns: Union[str, List[str]],
                 target_columns: Union[str, List[str]]) -> pd.DataFrame:
    """
    Adds copy of the source columns to the dataframe as target columns. If there are no source columns in the df, throws
    an exception. Source and target columns can be specified as a list of columns either single column.
    """
    try:
        if isinstance(source_columns, list):
            for i in range(len(source_columns)):
                df[target_columns[i]] = df[source_columns[i]].copy()
        else:
            df[target_columns] = df[source_columns].copy()
    except Exception:
        logging.error("There are no source columns in the data")
    return df


def drop_columns(df: pd.DataFrame, columns: Union[str, List[str]]) -> pd.DataFrame:
    """
    Drops columns. Can be one column or list of columns. If columns to drop doesn't exist, does nothing.
    """
    if isinstance(columns, list):
        for column in columns:
            df = df.drop(columns=column, errors='ignore')
    else:
        df = df.drop(columns=columns, errors='ignore')
    return df


def filter_rows(df: pd.DataFrame, filter_function: Callable, column: str = None) -> pd.DataFrame:
    """
    Applies custom filter function to the dataframe. E.g.

        def my_filter(row):
            return row['VALUES'] == '15 Sep 2017'

    If specific column to filter are given, pandas boolean functions can be used as filter functions:
    pd.isna, pd.notnull etc.
    """
    if column is not None:
        df = df[df[column].apply(filter_function)]
    else:
        df = df[df.apply(filter_function, axis=1)]
    return df


def set_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Leaves only columns from the list user gives and/or renames them.
    """
    if len(columns) < len(df.columns):
        df = df[columns]
    else:
        df = df.reindex(columns=columns)
    return df


def unpivot(df: pd.DataFrame, id_columns: List[str], value_columns: List[str], key: str = 'variable',
            value_name: str = 'value') -> pd.DataFrame:
    """
    Unpivots a dataFrame from wide to long format, optionally leaving identifiers set.
    E.g.
    "key": "variable", // Column name for the new keys column in the transformed dataframe;
    "value": "value", // Column name for the new values column in the transformed dataframe;
    "value_columns": ["height", "weight"], // Column names from the input data frame that will be converted to key/value pairs.
    """
    df = df.melt(id_vars=id_columns, value_vars=value_columns, var_name=key, value_name=value_name)
    return df


def unpivot2_bulk(
        df: pd.DataFrame,
        id_columns: List[str],
        config: Dict[str, TUnpivotColumnConfig],
        attribute_columns: Optional[List[str]] = None,
        key_column: str = 'variable',
        value_column: str = 'value',
        filter_empty_values: bool = True) -> pd.DataFrame:
    """
    Unpivots a dataFrame from wide to long format, optionally leaving identifiers set.
    """
    # Normalize imput (support alt syntax)
    for value in config:
        if isinstance(config[value], list):
            key = config[value][0]
            key = value if key is None else key
            attrs = config[value][1:]
            attrs = [{"col": attr[0]} if isinstance(attr, list) else {"val": attr} for attr in attrs]
            config[value] = {"key": key, "attr": attrs}

    # Process dataframe
    dfus = []
    for value in config:
        dfu = unpivot2(
            df=df, id_columns=id_columns, value=value, key_value=config[value].get("key", None),
            attribute_values=config[value].get("attr", None), attribute_columns=attribute_columns,
            key_column=key_column, value_column=value_column,
            filter_empty_values=filter_empty_values)
        dfus.append(dfu)

    return pd.concat(dfus, ignore_index=True)


def unpivot2(
        df: pd.DataFrame,
        id_columns: List[str],
        value: str,
        key_value: Optional[str] = None,  # equals to value_column by default
        attribute_values: Optional[List[TUnpivotAttr]] = None,  # no attributes by default
        attribute_columns: Optional[List[str]] = None,  # [ "attr1", "attr2", ..] by default
        key_column: str = 'variable',
        value_column: str = 'value',
        filter_empty_values: bool = True) -> pd.DataFrame:
    """
    Unpivots a dataFrame from wide to long format, optionally leaving identifiers set.
    """
    # Normalize inputs
    key_value = value if key_value is None else key_value
    attribute_values = [] if attribute_values is None else attribute_values
    attribute_columns = ["attr" + str(i) for i in range(1, len(attribute_values) + 1)] \
        if attribute_columns is None else attribute_columns

    # Filter value column
    df_values = df[value].copy()
    mask = None
    if filter_empty_values:
        mask = df_values.apply(func=lambda x: pd.notnull(x) and x != "")
        df_values = df_values[mask].copy()
    apply_mask = len(df) != len(df_values)

    # Process metadata, key, value columns
    dfr = df.loc[mask, id_columns].copy() \
        if apply_mask else df[id_columns].copy()
    dfr[key_column] = key_value
    dfr[value_column] = df_values

    # Process attribute columns
    attr_len = len(attribute_values)
    for i in range(0, len(attribute_columns)):
        attribute_value = attribute_values[i] if i < attr_len else {"val": None}
        attribute_column = attribute_columns[i]
        if "val" in attribute_value:
            dfr[attribute_column] = attribute_value["val"]
        elif "col" in attribute_value:
            dfr[attribute_column] = df.loc[mask, attribute_value["col"]].copy() \
                if apply_mask else df[attribute_value["col"]].copy()

    return dfr


def join(left: pd.DataFrame, right: pd.DataFrame, columns: Union[List[str], Dict[str, str]],
         left_on: Union[List[str], str] = None, right_on: Union[List[str], str] = None,
         on: Union[List[str], str] = None, right_filter: Dict[str, str] = None,
         how: str = 'left', suffix: str = None, **kwargs) -> pd.DataFrame:
    """
    Joins two dataframes. Values of the right can be filtered by "right filter@ parameter which is a dictionary \
    {column: value}. User can rename columns after joining by setting a dictionary to "columns" parameter or choose
    which columns to leave after joining by setting a list of columns. Default suffix is "_JOIN", but it can be change
    in "suffix" parameter.
    If columns to join on have the same names in both dataframes, use 'on' param, if names are different - use 'left_on'
    and 'right_on'
    """
    num_rows_before = len(left)

    def filter(row: pd.Series) -> bool:
        if right_filter:
            for column, value in right_filter.items():
                if row[column] != value:
                    return False
        return True

    def column_hit_and_miss(column_names: List[str], df) -> tuple:
        column_hit, column_miss = [], []
        for column in column_names:
            (column_miss, column_hit)[column in df].append(column)
        return column_hit, column_miss

    right = right[right.apply(filter, axis=1)]

    # Keep only columns we're interested in (all from left and some from right)
    columns_keep = left.columns.tolist()
    column_hit, column_miss = column_hit_and_miss(columns if isinstance(columns, list) else list(columns.keys()), right)

    forced_suffix = '_JOIN'
    right = right.rename(columns={key: key + forced_suffix for key in right.columns})

    if on is not None:
        left_on = on
        right_on = [column + forced_suffix for column in on]
    else:
        right_on = [column + forced_suffix for column in right_on]

    df = left.merge(right, right_on=right_on, left_on=left_on, how=how, suffixes=[None, "_JOIN"], copy=False,
                    **kwargs)

    suffix = '_JOIN' if suffix is None else suffix

    # Columns can be array (no rename) or object (auto rename)
    if isinstance(columns, list):
        # Leave columns from the right by the columns list
        # Add suffix to the column name if this introduces column collision
        column_renames = {column + forced_suffix: (column + suffix if column in left else column)
                          for column in column_hit if column + forced_suffix in df}
    else:
        # Leave columns from the right and rename by the columns dict
        # Add suffix to the column name if this introduces column collision
        column_renames = {
            column + forced_suffix: (columns[column] + suffix if columns[column] in left else columns[column])
            for column in column_hit if column + forced_suffix in df}

    columns_keep += list(column_renames.keys())
    df = df[columns_keep]
    df = rename_columns(df, column_renames)

    for column in df.columns:
        df[column] = _replace_nan_to_None(df[column])

    if len(column_miss):
        column_miss = ', '.join(column_miss)
        logging.warning(f'The requested columns are absent in the joined data: {column_miss}')

    return df


# todo add column name
def apply(df: pd.DataFrame, func: Callable) -> pd.DataFrame:
    """
    Applies custom function which gives a Series as an input.
    """
    return df.apply(func, axis=1, result_type='reduce')


def add_conditional_value(df: pd.DataFrame, value: Any, condition: Callable,
                          output_column: str, default_value: Any = False) -> pd.DataFrame:
    """
    Adds values to column (new or existing) based on condition function.
    If you want not to replace cells that weren't found, set default_value to anything except False.
    """

    def condition_function(row: pd.Series):
        """
        Checks condition and set values regarding to it
        """
        if condition(row):
            row[output_column] = value
        else:
            if default_value is not False:
                row[output_column] = default_value
        return row

    df = df.apply(condition_function, axis=1)
    return df


def compute_ratio(df: pd.DataFrame, numerator_column: str, denominator_column: str, output_column_name: str,
                  num_decimals: int = 0) -> pd.DataFrame:
    """
    Converts numerator amd denominator columns to numeric format and divides numerator to denominator.
    """
    # todo do we need to convert to string back here?
    try:
        df[numerator_column] = pd.to_numeric(df[numerator_column], errors='coerce')
        df[denominator_column] = pd.to_numeric(df[denominator_column], errors='coerce')
        df[output_column_name] = df[numerator_column].divide(df[denominator_column]).round(num_decimals)
        df[output_column_name] = _replace_nan_to_None(df[output_column_name])
    except Exception:
        logging.error(f"Couldn't parse {numerator_column} or {denominator_column} column values to float")
    return df


def to_uppercase(df: pd.DataFrame, input_column_name: str, output_column_name: str = None):
    """
    Converts column values to string and uppercase.
    """
    if output_column_name is None:
        output_column_name = input_column_name
    df[input_column_name] = _convert_series_to_string(df[input_column_name])
    df[output_column_name] = df[input_column_name].str.upper()
    df[output_column_name] = _replace_nan_to_None(df[output_column_name])
    return df


def get_substring(df: pd.DataFrame, input_column_name: str, output_column_name: str = None, start_idx: int = 0,
                  end_idx: int = -1):
    """
    Converts column values to string and gets slice of string. If start or end indexes are empty, returns the input
    string.
    """
    if output_column_name is None:
        output_column_name = input_column_name
    df[input_column_name] = _convert_series_to_string(df[input_column_name])
    try:
        df[output_column_name] = df[input_column_name].str.slice(start_idx, end_idx)
    except Exception:
        df[output_column_name] = df[input_column_name].apply(slice(start_idx, end_idx))
    df[output_column_name] = _replace_nan_to_None(df[output_column_name])
    return df


def split(df: pd.DataFrame, input_column_name: str, output_column_name: str, delimiter: str,
          part_number: int = 0) -> pd.DataFrame:
    """
    Splits column by delimiter.
    """
    df[input_column_name] = _convert_series_to_string(df[input_column_name])
    df[output_column_name] = df[input_column_name].str.split(delimiter, expand=True)[part_number]
    df[output_column_name] = _replace_nan_to_None(df[output_column_name])
    if df[output_column_name].equals(df[input_column_name]):
        logging.error(
            f'Input column {input_column_name} and output column {output_column_name} are the same after splitting!')
    return df


def _replace_nan_to_None(series: pd.Series) -> pd.Series:
    return series.where(pd.notnull(series), None)


def _convert_series_to_string(series: pd.Series) -> pd.Series:
    if not series.map(type).all == str:
        series = series.apply(lambda x: str(x) if not pd.isnull(x) else None)
    return series


def _create_column_dict(columns: List[str], word: str) -> dict:
    columns_dict = {}
    for c in columns:
        if word not in c[:len(word) + 1]:
            columns_dict[c] = word + '_' + c
    return columns_dict
