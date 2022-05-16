import pandas as pd
from sdw4_mapping.transformations.preparation import apply
from sdw4_mapping.transformations.specific import add_race, add_bmi, add_ongo

RACE_DICT = {
    "RAIAN": "AMERICAN INDIAN OR ALASKA NATIVE",
    "RASIAN": "ASIAN",
    "RBLACK": "BLACK OR AFRICAN AMERICAN",
    "RNHOPI": "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER",
    "RWHITE": "WHITE",
    "RNREP": "NOT REPORTED",
    "RUNK": "UNKNOWN",
}


def test_add_race_one():
    test_df = pd.DataFrame([[0, None, 0, 1, 0, 0, 0], [1, '15 Sep 2017', 0, 0, 0, 1, 0]],
                           columns=['idx', 'VALUES', 'RAIAN', 'RASIAN', 'RBLACK', "RNHOPI", "RWHITE"])
    test_df = add_race(df=test_df, races=RACE_DICT)
    assert test_df['RACE'].values[0] == 'ASIAN'


def test_add_race_multiple():
    test_df = pd.DataFrame([[0, None, 0, 1, 1, 0, 0], [1, '15 Sep 2017', 0, 0, 0, 1, 0]],
                           columns=['idx', 'VALUES', 'RAIAN', 'RASIAN', 'RBLACK', "RNHOPI", "RWHITE"])
    test_df = add_race(df=test_df, races=RACE_DICT)
    assert test_df['RACE'].values[0] == 'MULTIPLE'


def test_add_bmi():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'height': [62, 186],
        'weight': [116, 85],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm']
    })

    df = add_bmi(test_df, column_height='height', column_weight='weight', column_weight_units='weight_units',
                 column_height_units='height_units')
    assert abs(float(df['BMI'].values.tolist()[1]) - 24.6) < 0.01
    assert abs(float(df['BMI'].values.tolist()[0]) - 21.2) < 0.01


def test_add_ongo():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'height': [62, 186],
        'weight': [116, 85],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm'],
        'DMENDTC': ['2002-01-06', '']
    })
    df = add_ongo(test_df, prefix='DM')
    assert df['DMONGO'].values.tolist() == ['N', 'Y']


def test_apply():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm'],
        'DMENDTC': ['2002-01-06', '']
    })

    def foo(row: pd.Series) -> pd.Series:
        if row['name'] != "":
            row['name'] = row['name'] + ' Doe'
        return row

    df = apply(test_df, foo)
    assert df['name'].values.tolist() == ['Alice Doe', 'Bob Doe']

test_add_ongo()