import numpy as np
from dateutil.relativedelta import relativedelta
from sdw4_mapping.transformations.date_conversions import *


def test_add_age_from_birth_year_successful():
    year = datetime.today().year
    age_df = pd.DataFrame([[0, '1992'], [1, '1994'], [2, None]], columns=['Index', 'BIRTH'])
    age_df = add_age_from_birth_year(age_df, 'BIRTH')
    assert age_df['AGE'].values.tolist() == [str(year - 1992), str(year - 1994), None]


def test_add_age_from_birth_date_successful():
    dates = ['1992-04-01',
             '1994-10-02',
             None]
    age_df = pd.DataFrame({
        'idx': list(range(0, 3)),
        'BIRTH': dates,
    })
    age_df = add_age_from_birth_date(age_df, 'BIRTH')
    first_age = relativedelta(date.today(), convert_to_dt(dates[0])).years
    second_age = relativedelta(date.today(), convert_to_dt(dates[1])).years
    assert age_df['AGE'].values.tolist() == [str(first_age), str(second_age), None]  # change date if necessary


def test_add_duration_successful():
    test_df = pd.DataFrame([[0, '2018-06-01', '2020-06-02'], [1, '2019-06-01', '2020-06-02'], [2, '2019-06-01', None]],
                           columns=['Index', 'START_DATE', 'END_DATE'])
    test_df = add_duration(test_df, 'START_DATE', 'END_DATE')
    assert test_df['DURATION'].values.tolist() == [str(732), str(367), None]


def test_add_dy_successful():
    df = pd.DataFrame([[0, '01 Jan 2016', '29 Nov 2017']], columns=['idx', 'CMSTDTC', 'RFSTDTC'])
    df = add_dy(df=df, dtc_column='CMSTDTC')
    assert df['CMSTDY'].values.tolist() == ['-698']


def test_convert_date_smart():
    df = pd.DataFrame({
        'idx': [0, 1, 2, 3, 4, 5, 6, 7],
        'date': ['01 Jan 2016', '2020', '2012-06-01', '2012-06-01T00:00:00', '29 Nov 2017', 'Fri, 12 Dec 2014 10:55:50',
                 '1912', '02/02'],
    })
    df = convert_date_smart(df, date_column='date')
    dates = df['date'].values
    assert dates[0] == np.datetime64(datetime(2016, 1, 1))
    assert dates[1] == np.datetime64(datetime(2020, 1, 1))
    assert dates[2] == np.datetime64(datetime(2012, 6, 1))
    assert dates[3] == np.datetime64(datetime(2012, 6, 1))
    assert dates[4] == np.datetime64(datetime(2017, 11, 29))
    assert dates[5] == np.datetime64(datetime(2014, 12, 12, 10, 55, 50))
    assert dates[6] == np.datetime64(datetime(1912, 1, 1))
    assert dates[7] == np.datetime64(datetime(2021, 2, 2))


def test_convert_date_smart_only_date():
    df = pd.DataFrame({
        'idx': [0, 1, 2, 3, 4, 5, 6],
        'date': ['01 Jan 2016', '2020', '2012-06-01', '2012-06-01T00:00:00', '29 Nov 2017', 'Fri, 12 Dec 2014 10:55:50',
                 '1912'],
    })
    df = convert_date_smart(df, date_column='date', only_date=True)
    dates = df['date'].values
    assert dates[0] == date(2016, 1, 1)
    assert dates[1] == date(2020, 1, 1)
    assert dates[2] == date(2012, 6, 1)
    assert dates[3] == date(2012, 6, 1)
    assert dates[4] == date(2017, 11, 29)
    assert dates[5] == date(2014, 12, 12)
    assert dates[6] == date(1912, 1, 1)


def test_impute_partial_date():
    date_strings = ['--Jan2016',
                    '02---2016',
                    '--Jun----',
                    '--/FEB/2016',
                    '29/---/2017',
                    '05/Jan/----',
                    '---Jan-2016',
                    '29-----2017',
                    '05-Jun-----',
                    '2020-07---',
                    '2018-----',
                    '29SEP2020:16:23:45.733',
                    '15 MAY 2020',
                    'UN JUL 2020',
                    '2020',
                    'UN UNK 2016',
                    '30-May-70']
    test_df = pd.DataFrame({
        'idx': list(range(0, 17)),
        'ENDTC': date_strings,
        'STARTDTC': date_strings,
    })
    test_df = convert_date_smart(test_df, date_column='ENDTC')
    test_df = convert_date_smart(test_df, date_column='STARTDTC')

    assert test_df.loc[0, 'ENDTC'] == datetime(year=2016, month=1, day=31)
    assert test_df.loc[1, 'ENDTC'] == datetime(year=2016, month=12, day=2)
    assert test_df.loc[2, 'ENDTC'] == datetime(year=2020, month=6, day=30)
    assert test_df.loc[3, 'ENDTC'] == datetime(year=2016, month=2, day=29)
    assert test_df.loc[4, 'ENDTC'] == datetime(year=2017, month=12, day=29)
    assert test_df.loc[5, 'ENDTC'] == datetime(year=2021, month=1, day=5)
    assert test_df.loc[6, 'ENDTC'] == datetime(year=2016, month=1, day=31)
    assert test_df.loc[7, 'ENDTC'] == datetime(year=2017, month=12, day=29)
    assert test_df.loc[8, 'ENDTC'] == datetime(year=2021, month=6, day=5)
    assert test_df.loc[9, 'ENDTC'] == datetime(year=2020, month=7, day=31)
    assert test_df.loc[10, 'ENDTC'] == datetime(year=2018, month=12, day=31)
    assert test_df.loc[11, 'ENDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[12, 'ENDTC'] == datetime(year=2020, month=5, day=15)
    assert test_df.loc[13, 'ENDTC'] == datetime(year=2020, month=7, day=31)
    assert test_df.loc[14, 'ENDTC'] == datetime(year=2020, month=12, day=31)
    assert test_df.loc[15, 'ENDTC'] == datetime(year=2016, month=12, day=31)
    assert test_df.loc[16, 'ENDTC'] == datetime(year=1970, month=5, day=30)

    assert test_df.loc[0, 'STARTDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[1, 'STARTDTC'] == datetime(year=2016, month=1, day=2)
    assert test_df.loc[2, 'STARTDTC'] == datetime(year=2020, month=6, day=1)
    assert test_df.loc[3, 'STARTDTC'] == datetime(year=2016, month=2, day=1)
    assert test_df.loc[4, 'STARTDTC'] == datetime(year=2017, month=1, day=29)
    assert test_df.loc[5, 'STARTDTC'] == datetime(year=2021, month=1, day=5)
    assert test_df.loc[6, 'STARTDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[7, 'STARTDTC'] == datetime(year=2017, month=1, day=29)
    assert test_df.loc[8, 'STARTDTC'] == datetime(year=2021, month=6, day=5)
    assert test_df.loc[9, 'STARTDTC'] == datetime(year=2020, month=7, day=1)
    assert test_df.loc[10, 'STARTDTC'] == datetime(year=2018, month=1, day=1)
    assert test_df.loc[11, 'STARTDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[12, 'STARTDTC'] == datetime(year=2020, month=5, day=15)
    assert test_df.loc[13, 'STARTDTC'] == datetime(year=2020, month=7, day=1)
    assert test_df.loc[14, 'STARTDTC'] == datetime(year=2020, month=1, day=1)
    assert test_df.loc[15, 'STARTDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[16, 'STARTDTC'] == datetime(year=1970, month=5, day=30)


def test_convert_iso():
    test_df = pd.DataFrame({
        'idx': [0, 1],
        'ENDTC': ['2019-09-26T07:58:30.996+0200', '29SEP2020:16:23:45.733'],
        'STARTDTC': ['2019-09-26T07:58:30.996+0200', '29SEP2020:16:23:45.733'],
    })
    test_df = convert_date_smart(test_df, date_column='ENDTC')
    test_df = convert_date_smart(test_df, date_column='STARTDTC')
    assert test_df.loc[0, 'STARTDTC'] == datetime(year=2019, month=9, day=26, hour=7, minute=58,
                                                  second=30)
    assert test_df.loc[0, 'ENDTC'] == datetime(year=2019, month=9, day=26, hour=7, minute=58,
                                               second=30)


def test_convert_start_end():
    date_strings = ['--Jan2016',
                    '02---2016',
                    '--Jun----',
                    '--/FEB/2016',
                    '29/---/2017',
                    '05/Jan/----',
                    '---Jan-2016',
                    '29-----2017',
                    '05-Jun-----',
                    '2020-07---',
                    '2018-----',
                    '29SEP2020:16:23:45.733',
                    '15 MAY 2020',
                    'UN JUL 2020',
                    '2020',
                    'UN UNK 2016',
                    '30-May-70',
                    '16-Mar-45']

    test_df = pd.DataFrame({
        'idx': list(range(0, 18)),
        'ENDTC': date_strings,
        'STARTDTC': date_strings,
    })
    test_df = convert_date_smart(test_df, date_column='ENDTC', start=True)
    test_df = convert_date_smart(test_df, date_column='STARTDTC', start=False)

    assert test_df.loc[0, 'STARTDTC'] == datetime(year=2016, month=1, day=31)
    assert test_df.loc[1, 'STARTDTC'] == datetime(year=2016, month=12, day=2)
    assert test_df.loc[2, 'STARTDTC'] == datetime(year=2020, month=6, day=30)
    assert test_df.loc[3, 'STARTDTC'] == datetime(year=2016, month=2, day=29)
    assert test_df.loc[4, 'STARTDTC'] == datetime(year=2017, month=12, day=29)
    assert test_df.loc[5, 'STARTDTC'] == datetime(year=2021, month=1, day=5)
    assert test_df.loc[6, 'STARTDTC'] == datetime(year=2016, month=1, day=31)
    assert test_df.loc[7, 'STARTDTC'] == datetime(year=2017, month=12, day=29)
    assert test_df.loc[8, 'STARTDTC'] == datetime(year=2021, month=6, day=5)
    assert test_df.loc[9, 'STARTDTC'] == datetime(year=2020, month=7, day=31)
    assert test_df.loc[10, 'STARTDTC'] == datetime(year=2018, month=12, day=31)
    assert test_df.loc[11, 'STARTDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[12, 'STARTDTC'] == datetime(year=2020, month=5, day=15)
    assert test_df.loc[13, 'STARTDTC'] == datetime(year=2020, month=7, day=31)
    assert test_df.loc[14, 'STARTDTC'] == datetime(year=2020, month=12, day=31)
    assert test_df.loc[15, 'STARTDTC'] == datetime(year=2016, month=12, day=31)
    assert test_df.loc[16, 'STARTDTC'] == datetime(year=1970, month=5, day=30)
    assert test_df.loc[17, 'STARTDTC'] == datetime(year=1945, month=3, day=16)

    assert test_df.loc[0, 'ENDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[1, 'ENDTC'] == datetime(year=2016, month=1, day=2)
    assert test_df.loc[2, 'ENDTC'] == datetime(year=2020, month=6, day=1)
    assert test_df.loc[3, 'ENDTC'] == datetime(year=2016, month=2, day=1)
    assert test_df.loc[4, 'ENDTC'] == datetime(year=2017, month=1, day=29)
    assert test_df.loc[5, 'ENDTC'] == datetime(year=2021, month=1, day=5)
    assert test_df.loc[6, 'ENDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[7, 'ENDTC'] == datetime(year=2017, month=1, day=29)
    assert test_df.loc[8, 'ENDTC'] == datetime(year=2021, month=6, day=5)
    assert test_df.loc[9, 'ENDTC'] == datetime(year=2020, month=7, day=1)
    assert test_df.loc[10, 'ENDTC'] == datetime(year=2018, month=1, day=1)
    assert test_df.loc[11, 'ENDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[12, 'ENDTC'] == datetime(year=2020, month=5, day=15)
    assert test_df.loc[13, 'ENDTC'] == datetime(year=2020, month=7, day=1)
    assert test_df.loc[14, 'ENDTC'] == datetime(year=2020, month=1, day=1)
    assert test_df.loc[15, 'ENDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[16, 'ENDTC'] == datetime(year=1970, month=5, day=30)
    assert test_df.loc[17, 'ENDTC'] == datetime(year=1945, month=3, day=16)


def test_convert_format():
    date_strings = ['29SEP2020:16:23:45.733',
                    '29SEP2020:16:23:45.733',
                    '29SEP2020:16:23:45.733',
                    ]

    date_strings_1 = ['--Jan2016',
                      '--Jan2016',
                      '--Jan2016',
                      ]

    date_strings_2 = ['--Jan2016',
                      '29SEP2020:16:23:45.733',
                      'UN UNK 2016']
    test_df = pd.DataFrame({
        'idx': list(range(0, 3)),
        'ENDTC': date_strings_1,
        'STARTDTC': date_strings,
        'DTC': date_strings_2
    })
    test_df = convert_date_format(test_df, date_column='ENDTC', date_format='--%b%Y')
    test_df = convert_date_format(test_df, date_column='STARTDTC', date_format='%d%b%Y:%H:%M:%S.%f')
    test_df = convert_date_format(test_df, date_column='DTC', date_format='%d%b%Y:%H:%M:%S.%f')

    assert test_df.loc[0, 'ENDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[1, 'ENDTC'] == datetime(year=2016, month=1, day=1)
    assert test_df.loc[2, 'ENDTC'] == datetime(year=2016, month=1, day=1)

    assert test_df.loc[0, 'STARTDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[1, 'STARTDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)
    assert test_df.loc[2, 'STARTDTC'] == datetime(year=2020, month=9, day=29, hour=16, minute=23, second=45)

    assert test_df.loc[0, 'DTC'] == date_strings_2[0]
    assert test_df.loc[1, 'DTC'] == date_strings_2[1]
    assert test_df.loc[2, 'DTC'] == date_strings_2[2]


def test_convert_date_smart_none():
    date_strings = [None,
                    None,
                    None
                    ]

    date_strings_1 = ['',
                      '',
                      ''
                      ]

    test_df = pd.DataFrame({
        'idx': list(range(0, 3)),
        'DTC': date_strings,
        'DTC_1': date_strings_1
    })
    test_df = convert_date_smart(test_df, date_column='DTC', start=True)
    test_df = convert_date_smart(test_df, date_column='DTC_1', start=True)
    assert all(test_df['DTC'].values == [None, None, None])
    assert all(test_df['DTC_1'].values == [None, None, None])


def test_convert_date_smart_float():
    date_strings = ['1.0',
                    '1.0',
                    '0.0'
                    ]

    test_df = pd.DataFrame({
        'idx': list(range(0, 3)),
        'DTC': date_strings,
    })
    test_df = convert_date_smart(test_df, date_column='TC', start=True)  # try-catсh is date column not in the data
    test_df = convert_date_smart(test_df, date_column='DTC', start=True)
    assert all(test_df['DTC'].values == date_strings)
