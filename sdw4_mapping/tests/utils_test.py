import os

import pandas as pd
from sdw4_mapping.common.utils import save_to_csv, StringConverter
from sdw4_mapping.core.study import Study


def test_save_to_csv():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm'],
        'DMENDTC': ['2002-01-06', '']
    })
    output_folder = './data/output/'
    output_domain = 'None'
    save_to_csv(test_df, domain_name=output_domain, output_folder=output_folder)
    assert os.path.exists('./data/output/None.csv')
    assert pd.read_csv('./data/output/None.csv')['height_units'].values.tolist() == ['in', 'cm']


test_study = Study(id='test_study')


def test_string_converter():
    test_df = pd.read_csv(test_study.input_folder + 'AE.csv', converters=StringConverter())
    assert isinstance(test_df[test_df.columns[4]][0], str)
    assert isinstance(test_df[test_df.columns[7]][0], str)
