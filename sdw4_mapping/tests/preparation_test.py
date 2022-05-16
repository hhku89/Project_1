import numpy as np
import pandas as pd
from sdw4_mapping.transformations.preparation import map_column, add_column_constant, rename_columns, copy_columns, \
    drop_columns, filter_rows, unpivot, set_columns, join, add_conditional_value, compute_ratio, split, get_substring, \
    to_uppercase, unpivot2_bulk, unpivot2


def test_rename_columns_successful():
    test_df = pd.DataFrame([[0, '15 Sep 2018'], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'DAT'])
    test_df = rename_columns(df=test_df, columns={'idx': 'IND', 'DAT': 'DATE'})
    assert test_df.columns.tolist() == ['IND', 'DATE']


def test_rename_columns_identical_successful():
    test_df = pd.DataFrame([[0, '15 Sep 2018'], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'DAT'])
    test_df = rename_columns(df=test_df, columns={'idx': 'DAT'})
    assert test_df.columns.tolist() == ['DAT']

    test_df = rename_columns(df=test_df, columns={'DAT': 'DAT'})
    assert test_df.columns.tolist() == ['DAT']

    # That rename shouldn't work because column clashing is forbidden for inplace=False
    test_df['new'] = [1, 2, 'hello']
    test_df = rename_columns(df=test_df, columns={'new': 'DAT'}, inplace=False)
    assert test_df.columns.tolist() == ['DAT', 'new']


def test_add_column_constant_successful():
    test_df = pd.DataFrame([[0, '15 Sep 2018', 'a'], [1, '15 Sep 2017', 'b'], [2, None, 'c']],
                           columns=['idx', 'DAT', 'CONST'])
    test_df = add_column_constant(df=test_df, column='CONST', value=1, inplace=True)
    assert test_df['CONST'].values[0] == 1
    test_df = add_column_constant(df=test_df, column='CONST', value=2, inplace=False)
    assert test_df['CONST_1'].values[0] == 2


def test_map_column_successful():
    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = map_column(df=test_df, source_column='VALUES', dictionary={None: 'VALUE'})
    assert test_df['VALUES'].values[0] == 'VALUE'


def test_map_column_default():
    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = map_column(df=test_df, source_column='VALUES', dictionary={None: 'VALUE'}, default_value=None)
    assert test_df['VALUES'].values[0] == 'VALUE'
    assert test_df['VALUES'].values[1] is None
    assert test_df['VALUES'].values[2] == 'VALUE'

    test_df = map_column(df=test_df, source_column='VALUES', dictionary={None: 'VALUE'})
    assert test_df['VALUES'].values[0] == 'VALUE'
    assert test_df['VALUES'].values[1] == 'VALUE'
    assert test_df['VALUES'].values[2] == 'VALUE'


def test_copy_columns_many_successful():
    test_df = pd.DataFrame([[0, None, None], [1, '15 Sep 2017', None], [2, None, None]],
                           columns=['idx', 'VALUES', 'VALUES_1'])
    test_df = copy_columns(df=test_df, source_columns=['VALUES', 'VALUES_1'], target_columns=['VALUES_2', 'VALUES_3'])
    assert test_df['VALUES'].values[0] == test_df['VALUES_2'].values[0]
    assert test_df['VALUES_1'].values[0] == test_df['VALUES_3'].values[0]


def test_copy_columns_one_successful():
    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = copy_columns(df=test_df, source_columns='VALUES', target_columns='VALUES_2')
    assert test_df['VALUES'].values[0] == test_df['VALUES_2'].values[0]


def test_drop_columns_many_successful():
    test_df = pd.DataFrame([[0, None, None], [1, '15 Sep 2017', None], [2, None, None]],
                           columns=['idx', 'VALUES', 'VALUES_1'])
    test_df = drop_columns(df=test_df, columns=['VALUES', 'VALUES_1'])
    assert test_df.columns.tolist() == ['idx']


def test_drop_columns_one_successful():
    test_df = pd.DataFrame([[0, None, None], [1, '15 Sep 2017', None], [2, None, None]],
                           columns=['idx', 'VALUES', 'VALUES_1'])
    test_df = drop_columns(df=test_df, columns=['VALUES_1'])
    assert test_df.columns.tolist() == ['idx', 'VALUES']


def test_filter_successful():
    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = filter_rows(df=test_df, filter_function=pd.notnull, column='VALUES')
    assert test_df['VALUES'].values[0] == '15 Sep 2017'


def test_filter_function_successful():
    def my_filter(row):
        return row['VALUES'] == '15 Sep 2017'

    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = filter_rows(df=test_df, filter_function=my_filter)
    assert test_df['VALUES'].values[0] == '15 Sep 2017'


def test_filter_lambda_successful():
    test_df = pd.DataFrame([[0, None], [1, '15 Sep 2017'], [2, None]], columns=['idx', 'VALUES'])
    test_df = filter_rows(df=test_df, filter_function=lambda x: x['VALUES'] == '15 Sep 2017')
    assert test_df['VALUES'].values[0] == '15 Sep 2017'


def test_unpivot():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'height': [62, 186],
        'weight': [116, 85],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm']
    })

    df = unpivot(df=test_df, id_columns=['name', 'weight_units', 'height_units'], value_columns=['height', 'weight'])
    assert df['variable'].values.tolist() == ['height', 'height', 'weight', 'weight']
    assert df['value'].values.tolist() == [62, 186, 116, 85]
    assert df['name'].values.tolist() == ['Alice', 'Bob', 'Alice', 'Bob']
    assert df['weight_units'].values.tolist() == ['lb', 'kg', 'lb', 'kg']
    assert df['height_units'].values.tolist() == ['in', 'cm', 'in', 'cm']


def test_set_columns():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'height': [62, 186],
        'weight': [116, 85],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm']
    })
    test_df = set_columns(test_df, columns=['name', 'height', 'weight'])
    assert len(test_df.columns) == 3


def test_set_columns_reindex():
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'height': [62, 186],
        'weight': [116, 85],
        'weight_units': ['lb', 'kg'],
        'height_units': ['in', 'cm']
    })
    new_columns = ['weight', 'name', 'height', 'height_units', 'weight_units']
    test_df = set_columns(test_df, columns=new_columns)
    assert test_df.columns.tolist() == new_columns
    assert test_df.loc[1, 'name'] == 'Bob'


def test_join():
    LEFT_PATH = './data/input/SV.csv'
    RIGHT_PATH = './data/input/DM.csv'
    left = pd.read_csv(LEFT_PATH)
    right = pd.read_csv(RIGHT_PATH)

    df = join(left, right,
              columns=["EXPRTDT"],
              on=["SubjectName"],
              )
    assert "EXPRTDT" in df.columns

    df = join(
        left, right,
        columns=["EXPRTDT"],
        on=["SubjectName"],
        how='right',
        right_filter={
            "StudyEventOID": 10,
            "StudyEventRepeatKey": "10[1]",
            'LocationOID': 102
        },
    )
    assert sum(df['EXPRTDT'].isna()) == 0


def test_join_rename_collision():
    df1 = pd.DataFrame(
        {"idnum": ["1", "1", "2", "2", "3", "3"], "visit": ["A", "B", "A", "B", "A", "B"], "value": [1, 2, 3, 4, 5, 6]}
    )
    df2 = df1.copy()

    # Check no internal column collision
    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns={"value": "value_A"},
    )

    assert ['idnum', 'visit', 'value', 'value_A'] == list(df.columns)
    assert [1, 1, 3, 3, 5, 5] == list(df['value_A'])  # values from the right (df2)

    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns=["value"],
    )

    assert ['idnum', 'visit', 'value', 'value_JOIN'] == list(df.columns)
    assert [1, 1, 3, 3, 5, 5] == list(df['value_JOIN'])  # values from the right (df2)

    # Check no crash if column is absent
    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns={"nonsense": "value_A"},
    )

    assert ['idnum', 'visit', 'value'] == list(df.columns)

    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns=["nonsense"],
    )

    assert ['idnum', 'visit', 'value'] == list(df.columns)

    # Check intentionally overwrite a column from left
    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns=["value"],
        suffix=""
    )

    assert ['idnum', 'visit', 'value'] == list(df.columns)
    assert [1, 1, 3, 3, 5, 5] == list(df['value'])  # values from the right (df2)

    df = join(
        df1,
        df2,
        on=["idnum"],
        right_filter={"visit": "A"},
        columns={"value": "value"},
        suffix=""
    )

    assert ['idnum', 'visit', 'value'] == list(df.columns)
    assert [1, 1, 3, 3, 5, 5] == list(df['value'])  # values from the right (df2)


def test_join_left_right_on():
    left = pd.DataFrame({
        'idx': [0, 1, 2],
        'left_join': [62, None, 'example'],
        'values': [1, 2, 3]
    })
    right = pd.DataFrame({
        'idx': [0, 1, 2],
        'right_join': [62, '15 Sep 2017', 'example'],
        'values': [4, 5, 6]
    })

    df = join(left, right,
              columns=["values"],
              right_on=["right_join"],
              left_on=['left_join']
              )
    assert df['values_JOIN'].values.tolist() == [4.0, None, 6.0]


def test_add_conditional_value():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2],
        'VALUES': [62, '15 Sep 2017', 'example'],
        'VALUES_1': ['116', None, None],
    })

    def my_condition(row) -> bool:
        return row['VALUES'] == 'example'

    test_df = add_conditional_value(test_df, condition=my_condition, value=1, output_column='VALUES_1')
    assert test_df['VALUES_1'][2] == 1
    assert test_df['VALUES_1'][1] is None
    assert test_df['VALUES_1'][0] == '116'

    def my_condition(row):
        return row['VALUES_1'] == 1

    test_df = add_conditional_value(test_df, condition=my_condition, value='VALUES_1', output_column='VALUES_1',
                                    default_value=None)
    assert test_df['VALUES_1'][2] == 'VALUES_1'
    assert test_df['VALUES_1'][1] is None
    assert test_df['VALUES_1'][0] is None


def test_compute_ratio():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2],
        'NUMERATOR': [2, 4.246, 6],
        'DENOMINATOR': [2, 2, 2],
    })
    test_df = compute_ratio(df=test_df, denominator_column='DENOMINATOR', numerator_column='NUMERATOR',
                            output_column_name='RESULT', num_decimals=3)
    assert test_df['RESULT'].values.tolist() == [1, 2.123, 3]


def test_compute_ratio_strings():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'NUMERATOR': ['2', '4.246', None, 'hello'],
        'DENOMINATOR': [None, 2, 2, '3'],
    })
    test_df = compute_ratio(df=test_df, denominator_column='DENOMINATOR', numerator_column='NUMERATOR',
                            output_column_name='RESULT', num_decimals=3)
    assert test_df['RESULT'].values.tolist() == [None, 2.123, None, None]


def test_split():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'TO_SPLIT': [None, 'hello-1', 'hello-2', '3'],
    })
    test_df = split(df=test_df, input_column_name='TO_SPLIT', output_column_name='SPLITTED', delimiter='-',
                    part_number=0)
    assert test_df['SPLITTED'].values.tolist() == [None, 'hello', 'hello', '3']


def test_split_int():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'TO_SPLIT': [None, 1, 2, 4],
    })
    test_df = split(df=test_df, input_column_name='TO_SPLIT', output_column_name='SPLITTED', delimiter='-',
                    part_number=0)
    assert test_df['SPLITTED'].values.tolist() == [None, '1.0', '2.0', '4.0']


def test_split_same():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'TO_SPLIT': [None, '1', '2', '4'],
    })
    test_df = split(df=test_df, input_column_name='TO_SPLIT', output_column_name='SPLITTED', delimiter='-',
                    part_number=0)
    assert test_df['SPLITTED'].values.tolist() == [None, '1', '2', '4']


def test_get_substring():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'SOURCE': [None, 'hello-1', 'hello-2', 'hel'],
    })
    test_df = get_substring(test_df, input_column_name='SOURCE', start_idx=0, end_idx=5)
    assert test_df['SOURCE'].values.tolist() == [None, 'hello', 'hello', 'hel']


def test_to_uppercase():
    test_df = pd.DataFrame({
        'idx': [0, 1, 2, 3],
        'SOURCE': [None, 'hello-1', 'hello-2', 1],
    })
    test_df = to_uppercase(test_df, input_column_name='SOURCE')
    assert test_df['SOURCE'].values.tolist() == [None, 'HELLO-1', 'HELLO-2', '1']


def test_unpivot2_bulk1():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "geographical", "A", "x"],
            ["City1", "City3", "2", "nautical", "B", "y"],
            ["City2", "City3", "3", "exotic", "C", "z"],
        ],
        columns=["Loc1", "Loc2", "distance_miles", "mile_type", "relation", "relation_unit"]
    )
    dfu = unpivot2_bulk(
        df=df,
        config={
            "distance_miles": {
                "key": "distance",
                "attr": [
                    {"val": "miles"},
                    {"col": "mile_type"},
                ]
            },
            "relation": {
                "attr": [
                    {"col": "relation_unit"},
                ]
            },
        },
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 6
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'distance', '1', 'miles', 'geographical']
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'distance', '2', 'miles', 'nautical']
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'distance', '3', 'miles', 'exotic']
    assert dfu.loc[3].tolist() == ['City1', 'City2', 'relation', 'A', 'x', None]
    assert dfu.loc[4].tolist() == ['City1', 'City3', 'relation', 'B', 'y', None]
    assert dfu.loc[5].tolist() == ['City2', 'City3', 'relation', 'C', 'z', None]


def test_unpivot2_bulk1_alt_syntax():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "geographical", "A", "x"],
            ["City1", "City3", "2", "nautical", "B", "y"],
            ["City2", "City3", "3", "exotic", "C", "z"],
        ],
        columns=["Loc1", "Loc2", "distance_miles", "mile_type", "relation", "relation_unit"]
    )
    dfu = unpivot2_bulk(
        df=df,
        config={
            "distance_miles": ["distance", "miles", ["mile_type"]],
            "relation": [None, ["relation_unit"]],
        },
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 6
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'distance', '1', 'miles', 'geographical']
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'distance', '2', 'miles', 'nautical']
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'distance', '3', 'miles', 'exotic']
    assert dfu.loc[3].tolist() == ['City1', 'City2', 'relation', 'A', 'x', None]
    assert dfu.loc[4].tolist() == ['City1', 'City3', 'relation', 'B', 'y', None]
    assert dfu.loc[5].tolist() == ['City2', 'City3', 'relation', 'C', 'z', None]


def test_unpivot2_bulk2():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "", np.NaN, "A", "x"],
            ["City1", "City3", None, "2", "", "B", "y"],
            ["City2", "City3", np.NaN, None, "3", "B", "y"],
        ],
        columns=["Loc1", "Loc2", "distance_miles1", "distance_miles2", "distance_miles3", "relation", "relation_unit"]
    )
    dfu = unpivot2_bulk(
        df=df,
        config={
            "distance_miles1": {
                "key": "distance",
                "attr": [
                    {"val": "miles"},
                    {"val": "nautical"},
                ]
            },
            "distance_miles2": {
                "key": "distance",
                "attr": [
                    {"val": "miles"},
                    {"val": "geographical"},
                ]
            },
            "distance_miles3": {
                "key": "distance",
                "attr": [
                    {"val": "miles"},
                    {"val": "exotic"},
                ]
            },
            "relation": {
                "attr": [
                    {"col": "relation_unit"},
                ]
            },
        },
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 6
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'distance', '1', 'miles', 'nautical']
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'distance', '2', 'miles', 'geographical']
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'distance', '3', 'miles', 'exotic']
    assert dfu.loc[3].tolist() == ['City1', 'City2', 'relation', 'A', 'x', None]
    assert dfu.loc[4].tolist() == ['City1', 'City3', 'relation', 'B', 'y', None]
    assert dfu.loc[5].tolist() == ['City2', 'City3', 'relation', 'B', 'y', None]


def test_unpivot2_bulk2_alt_syntax():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "", np.NaN, "A", "x"],
            ["City1", "City3", None, "2", "", "B", "y"],
            ["City2", "City3", np.NaN, None, "3", "B", "y"],
        ],
        columns=["Loc1", "Loc2", "distance_miles1", "distance_miles2", "distance_miles3", "relation", "relation_unit"]
    )
    dfu = unpivot2_bulk(
        df=df,
        config={
            "distance_miles1": ["distance", "miles", "nautical"],
            "distance_miles2": ["distance", "miles", "geographical"],
            "distance_miles3": ["distance", "miles", "exotic"],
            "relation": [None, ["relation_unit"]],
        },
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 6
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'distance', '1', 'miles', 'nautical']
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'distance', '2', 'miles', 'geographical']
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'distance', '3', 'miles', 'exotic']
    assert dfu.loc[3].tolist() == ['City1', 'City2', 'relation', 'A', 'x', None]
    assert dfu.loc[4].tolist() == ['City1', 'City3', 'relation', 'B', 'y', None]
    assert dfu.loc[5].tolist() == ['City2', 'City3', 'relation', 'B', 'y', None]


def test_unpivot2_bulk3_alt_syntax():
    df = pd.DataFrame(
        [
            ["x001", 0.1, 21, "year", 102, "alpha", 80],
            ["x002", 0.2, 22, "year", 202, "beta", 80],
            ["x003", 0.3, 23, "year", 302, "gamma", 80],
        ],
        columns=["id", "misc", "obs1", "obs1_unit", "obs2", "obs2_unit", "weight"]
    )
    dfu = unpivot2_bulk(
        df=df,
        config={
            "obs1": [None, ["obs1_unit"]],
            "obs2": [None, ["obs2_unit"]],
            "weight": [None, "kg"],
        },
        id_columns=["id", "misc"],
        key_column="code",
        value_column="value",
        attribute_columns=["value_unit"],
    )
    assert list(dfu.columns) == ["id", "misc", "code", "value", "value_unit"]
    assert len(dfu) == 9
    print(dfu)
    assert dfu.loc[0].tolist() == ["x001", 0.1, "obs1", 21, "year"]
    assert dfu.loc[1].tolist() == ["x002", 0.2, "obs1", 22, "year"]
    assert dfu.loc[2].tolist() == ["x003", 0.3, "obs1", 23, "year"]
    assert dfu.loc[3].tolist() == ["x001", 0.1, "obs2", 102, "alpha"]
    assert dfu.loc[4].tolist() == ["x002", 0.2, "obs2", 202, "beta"]
    assert dfu.loc[5].tolist() == ["x003", 0.3, "obs2", 302, "gamma"]
    assert dfu.loc[6].tolist() == ["x001", 0.1, "weight", 80, "kg"]
    assert dfu.loc[7].tolist() == ["x002", 0.2, "weight", 80, "kg"]
    assert dfu.loc[8].tolist() == ["x003", 0.3, "weight", 80, "kg"]


def test_unpivot2_1():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "geographical", "A", "x"],
            ["City1", "City3", "2", "nautical", "B", "y"],
            ["City2", "City3", "3", "exotic", "C", "z"],
        ],
        columns=["Loc1", "Loc2", "distance_miles", "mile_type", "relation", "relation_unit"]
    )
    dfu = unpivot2(
        df=df,
        value="distance_miles",
        key_value="distance",
        attribute_values=[{"val": "miles"}, {"col": "mile_type"}],
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 3
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'distance', '1', 'miles', 'geographical']
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'distance', '2', 'miles', 'nautical']
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'distance', '3', 'miles', 'exotic']


def test_unpivot2_2():
    df = pd.DataFrame(
        [
            ["City1", "City2", "1", "geographical", "A", "x"],
            ["City1", "City3", "2", "nautical", "B", "y"],
            ["City2", "City3", "3", "exotic", "C", "z"],
        ],
        columns=["Loc1", "Loc2", "distance_miles", "mile_type", "relation", "relation_unit"]
    )
    dfu = unpivot2(
        df=df,
        value="relation",
        attribute_values=[{"col": "relation_unit"}],
        id_columns=["Loc1", "Loc2"],
        key_column="Key",
        value_column="Value",
        attribute_columns=["ValueUnit", "ValueUnitType"],
    )
    assert list(dfu.columns) == ["Loc1", "Loc2", "Key", "Value", "ValueUnit", "ValueUnitType"]
    assert len(dfu) == 3
    assert dfu.loc[0].tolist() == ['City1', 'City2', 'relation', 'A', 'x', None]
    assert dfu.loc[1].tolist() == ['City1', 'City3', 'relation', 'B', 'y', None]
    assert dfu.loc[2].tolist() == ['City2', 'City3', 'relation', 'C', 'z', None]
