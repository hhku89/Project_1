import os

import pandas as pd
from sdw4_mapping.common.utils import StringConverter
from sdw4_mapping.core.domain import Domain
from sdw4_mapping.core.study import Study

test_study = Study(id='test_study')


def test_domain_init():
    OUTPUT_FOLDER = './data/output/'

    test_domain = Domain(study=test_study, name='test', data_path=os.path.join(test_study.input_folder, 'SV.csv'))
    test_domain.rename_columns({'SubjectName': 'subjectId'})
    assert test_study.id == 'test_study'
    assert test_study.output_folder == OUTPUT_FOLDER
    assert test_domain.name == 'test'
    assert 'FormOID' in test_domain.data.columns

    return test_domain


def test_add_studyid_domain_usubjid():
    test_domain = test_domain_init()
    test_domain.add_studyid_domain_usubjid()
    assert test_domain.data['DOMAIN'][0] == test_domain.name
    assert test_domain.data['STUDYID'][0] == test_study.id


def test_add_studyid_domain_usubjid_with_siteid():
    test_domain = Domain(study=test_study, name='test', data_path=test_study.input_folder + 'LB.csv')
    test_domain.rename_columns({'SUBJID': 'subjectId'})
    test_domain.add_studyid_domain_usubjid()
    test_df = test_domain.data
    studyid = test_df["STUDYID"][0]
    siteid = test_df["SITEID"][0]
    subjid = test_df["subjectId"][0]
    assert test_domain.data['DOMAIN'][0] == test_domain.name
    assert test_domain.data["USUBJID"][0] == studyid + "-" + siteid + "-" + subjid


def test_string_converter():
    test_df = pd.read_csv(test_study.input_folder + 'AE.csv', converters=StringConverter())
    assert isinstance(test_df[test_df.columns[4]][0], str)
    assert isinstance(test_df[test_df.columns[7]][0], str)


def test_get_columns_and_print():
    test_domain = Domain(study=test_study, name='test')
    test_df = pd.read_csv(test_study.input_folder + 'AE.csv', converters=StringConverter())
    test_domain.data = test_df
    selected_df = test_domain[['Subject', 'RecordId']]
    test_domain.data = selected_df
    assert test_domain.data.columns.tolist() == ['Subject', 'RecordId']

    test_domain.data = selected_df.head(5)
    output_strings = 'Domain: test\nStudy: test_study\nColumns: Subject, RecordId\n  Subject RecordId'
    assert str(test_domain)[:76] == output_strings


test_get_columns_and_print()
