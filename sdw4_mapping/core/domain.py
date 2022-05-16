from __future__ import annotations

import logging
from typing import List, Union, Dict, Any, Callable, Optional

import pandas as pd
from sdw4_mapping.common.utils import save_to_csv, read_data
from sdw4_mapping.core.study import Study
from sdw4_mapping.transformations import preparation as sdw_pr, date_conversions as sdw_dt, specific as sdw_sp
from sdw4_mapping.transformations.preparation import TUnpivotAttr, TUnpivotColumnConfig


def add_usubjid(df: pd.DataFrame, subjid_column: str = 'subjectId', studyid_column: str = 'STUDYID',
                siteid_column: str = 'SITEID', usubjid_column: str = "USUBJID") -> pd.DataFrame:
    """
    Adds user and subject id columns to the data. Needs subjid and studyid columns, if it doesn't exist, ithrows an exception.
    """
    try:
        if siteid_column in df.columns:
            df[usubjid_column] = df[studyid_column] + "-" + df[siteid_column].astype(str) + "-" + df[
                subjid_column].astype(str)
        else:
            df[usubjid_column] = df[studyid_column] + "-" + df[subjid_column].astype(str)
    except Exception:
        logging.error('There are no some columns in data')
    return df


class Domain:
    """
    The Study Domain file is used to collect all operational data that pertain to a Human Subject Research Study
    (also known as a Clinical Study)
    This class inherits all methods from transformations module to operate with domain data.
    For more information about the methods of the class please check submodules.
    """

    def __init__(self, study: Study, name: str, data_path: str = None, usecols: List[str] = None) -> None:
        self.name = name
        self.study = study
        self.data = read_data(data_path=data_path, usecols=usecols)

    def __str__(self):
        df_info = 'Domain: ' + self.name + '\n' + \
                  'Study: ' + self.study.id + '\n' + \
                  'Columns: ' + ', '.join(self.data.columns.tolist()) + '\n'
        return df_info + str(self.data)

    def __getitem__(self, item: List[str]):
        return self.data[item]

    def add_studyid_domain_usubjid(self) -> None:
        """
        Adds user and subject id columns to the data.
        """
        # todo works long
        self.data['STUDYID'] = self.study.id
        self.data['DOMAIN'] = self.name
        self.data = add_usubjid(self.data)

    @classmethod
    def union(cls, domains: Union[List[Domain]]) -> Domain:
        """
        Combines the list of Domains to one Domain using pd.concat. If the list is empty, ithrows an exception.
        Name, study and columns of the new domain comes from the first domain in the list.
        """
        if len(domains) == 0:
            raise Exception("Domain.union requires at least one domain as input")

        frames = [domain.data for domain in domains]
        df = pd.concat(frames, ignore_index=True)

        domain = cls(domains[0].study, domains[0].name)
        domain.data = df

        return domain

    # data conversion
    def convert_date(self, date_column: str, start: bool = None, only_date: bool = False):
        """
        Converts whole column from string to datetime format.
        """
        self.data = sdw_dt.convert_date_smart(df=self.data, date_column=date_column, start=start, only_date=only_date)

    def convert_date_format(self, date_column: str, date_format: Union[List[str], str] = "%Y-%m-%d"):
        """
        Converts whole column from string to datetime format when exact format is known.
        """
        self.data = sdw_dt.convert_date_format(df=self.data, date_column=date_column, date_format=date_format)

    def add_age_from_birth_year(self, column_birth_year: str, column_age: str = "AGE"):
        """
        Calculates age as delta from birth year and adds age column.
        """

        self.data = sdw_dt.add_age_from_birth_year(df=self.data, column_birth_year=column_birth_year,
                                                   column_age=column_age)

    def add_age_from_birth_date(self, column_birth_date: str, column_age: str = "AGE",
                                date_format: str = "%Y-%m-%d"):
        """
        Calculates age from birth date and adds age column.
        """
        self.data = sdw_dt.add_age_from_birth_date(df=self.data, column_birth_date=column_birth_date,
                                                   column_age=column_age)

    def add_duration(self, column_start: str, column_end: str, column_dur: str = 'DURATION',
                     date_format: str = "%Y-%m-%d"):
        """
        Calculates and adds study duration column.
        """
        self.data = sdw_dt.add_duration(df=self.data, column_start=column_start, column_end=column_end,
                                        column_dur=column_dur)

    def add_dy(self, dtc_column, dy_column=None):
        """
        Calculates and adds column with study duration in days.
        """
        self.data = sdw_dt.add_dy(df=self.data, dtc_column=dtc_column, dy_column=dy_column)

    # preparation
    def rename_columns(self, columns: Union[Dict[str, str], dict]):
        """
        Renames columns.
        """
        self.data = sdw_pr.rename_columns(df=self.data, columns=columns)

    def add_column_constant(self, column: str, value: Any):
        """
        Adds column with a constant value.
        """
        self.data = sdw_pr.add_column_constant(df=self.data, column=column, value=value)

    def map_column(self, source_column: str, dictionary: Dict[Union[str, None], Union[str, None]],
                   default_value: Any = False):
        """
        Replaces values in the column, values are defined as a dictionary.
        """
        self.data = sdw_pr.map_column(df=self.data, source_column=source_column, dictionary=dictionary,
                                      default_value=default_value)

    def copy_columns(self, source_columns: Union[str, List[str]],
                     target_columns: Union[str, List[str]]):
        """
        Adds copy of the source columns to the data.
        """
        self.data = sdw_pr.copy_cloumns(df=self.data, source_columns=source_columns, target_columns=target_columns)

    def drop_columns(self, columns: Union[str, List[str]]):
        """
        Drops columns from data.
        """
        self.data = sdw_pr.drop_columns(df=self.data, columns=columns)

    def filter_rows(self, filter_function: Callable, column: str = None):
        """
        Filters data values by custom filter function.
        """
        self.data = sdw_pr.filter_rows(df=self.data, filter_function=filter_function, column=column)

    def apply(self, func: Callable):
        """
        Applies custom function.
        """
        self.data = sdw_pr.apply(df=self.data, func=func)

    def set_columns(self, columns: List[str]):
        """
        Leaves only columns from the list user gives and/or renames them.
        """
        self.data = sdw_pr.set_columns(df=self.data, columns=columns)

    def unpivot(self, id_columns: List[str], value_columns: List[str], key: str = 'variable',
                value_name: str = 'value'):
        """
        Unpivots domain data.
        """
        self.data = sdw_pr.unpivot(df=self.data, value_columns=value_columns, id_columns=id_columns, key=key,
                                   value_name=value_name)

    def unpivot2(self,
            id_columns: List[str],
            value: str,
            key_value: Optional[str] = None,  # equals to value_column by default
            attribute_values: Optional[List[TUnpivotAttr]] = None,  # no attributes by default
            attribute_columns: Optional[List[str]] = None,  # [ "attr1", "attr2", ..] by default
            key_column: str = 'variable',
            value_column: str = 'value',
            filter_empty_values: bool = True):
        """
        Unpivots domain data.
        """
        self.data = sdw_pr.unpivot2(df=self.data, id_columns=id_columns, value=value, key_value=key_value,
                                    attribute_values=attribute_values, attribute_columns=attribute_columns,
                                    key_column=key_column, value_column=value_column,
                                    filter_empty_values=filter_empty_values)

    def unpivot2_bulk(self,
        id_columns: List[str],
        config: Dict[str, TUnpivotColumnConfig],
        attribute_columns: Optional[List[str]] = None,
        key_column: str = 'variable',
        value_column: str = 'value',
        filter_empty_values: bool = True) -> pd.DataFrame:
        """
        Unpivots domain data.
        """
        self.data = sdw_pr.unpivot2_bulk(df=self.data, id_columns=id_columns, config=config,
                                    attribute_columns=attribute_columns,
                                    key_column=key_column, value_column=value_column,
                                    filter_empty_values=filter_empty_values)

    def join(self, right: Union[Domain, pd.DataFrame], on: Union[List[str], str],
             columns: Union[List[str], Dict[str, str]], right_filter: Dict[str, str] = None,
             how: str = 'left', suffix: str = None, **kwargs):
        """
        Joins two domains or the right could be pd.DataFrame also.
        """
        right_domain_name = 'N/A'
        if isinstance(right, Domain):
            right_domain_name = right.name
            right = right.data

        num_rows_before = len(self.data)
        self.data = sdw_pr.join(left=self.data, right=right, on=on, columns=columns, right_filter=right_filter, how=how,
                                suffix=suffix, **kwargs)
        num_rows_after = len(self.data)

        if num_rows_before != num_rows_after:
            logging.warning(
                f'After joining "{self.name}" (left) and "{right_domain_name}" (right): Row count changed from {num_rows_before} to {num_rows_after}.')

    def add_conditional_value(self, value: Any, condition: Callable, output_column: str, default_value: Any = False):
        """
        Adds values to column (new or existing) based on condition function.
        """
        self.data = sdw_pr.add_conditional_value(df=self.data, value=value, condition=condition,
                                                 output_column=output_column, default_value=default_value)

    def compute_ratio(self, numerator_column: str, denominator_column: str, output_column_name: str,
                      num_decimals: int = 0):
        """
        Converts numerator amd denominator columns to numeric format and divides numerator to denominator.
        """
        self.data = sdw_pr.compute_ratio(df=self.data, numerator_column=numerator_column,
                                         denominator_column=denominator_column, output_column_name=output_column_name,
                                         num_decimals=num_decimals)

    def to_uppercase(self, input_column_name: str, output_column_name: str = None):
        """
        Converts column values to string and uppercase.
        """
        self.data = sdw_pr.to_uppercase(df=self.data, input_column_name=input_column_name,
                                        output_column_name=output_column_name)

    def get_substring(self, input_column_name: str, output_column_name: str = None, start_idx: int = 0,
                      end_idx: int = -1):
        """
        Converts column values to string and gets slice of string. If start or end indexes are empty, returns the input
        string.
        """
        self.data = sdw_pr.get_substring(df=self.data, input_column_name=input_column_name,
                                         output_column_name=output_column_name, start_idx=start_idx, end_idx=end_idx)

    def split(self, input_column_name: str, output_column_name: str, delimiter: str,
              part_number: int = 0):
        """
        Splits column by delimiter.
        """
        self.data = sdw_pr.split(df=self.data, input_column_name=input_column_name,
                                 output_column_name=output_column_name, delimiter=delimiter,
                                 part_number=part_number)

    # specific methods
    def add_bmi(self, column_height: str = 'HEIGHT', column_weight: str = 'WEIGHT',
                column_bmi: str = 'BMI'):
        """
        Calculates BMI in US and metric units.
        """
        self.data = sdw_sp.add_bmi(df=self.data, column_height=column_height, column_weight=column_weight,
                                   column_bmi=column_bmi)

    def add_ongo(self, prefix: str):
        """
        Adds column with values if study still runs or not.
        """
        self.data = sdw_sp.add_ongo(df=self.data, prefix=prefix)

    def add_race(self, races: Dict[str, str]):
        """
        Adds column with race instead of dummy columns, uses dictionary to replace values.
        """
        self.data = sdw_sp.add_race(df=self.data, races=races)

    def to_csv(self, output_folder: str = None):
        """
        Saves domain data to file.
        """
        if output_folder is None:
            output_folder = self.study.output_folder
        save_to_csv(df=self.data, domain_name=self.name, output_folder=output_folder)
