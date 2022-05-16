import pandas as pd
from sdw4_mapping.transformations.date_conversions import convert_to_dt
from datetime import date


def make_tc(domain, column):
    df = domain.data
    df[column] = df[column].apply(lambda x: pd.NaT if x == "" else x)
    domain.convert_date(column)
    df = domain.data

    def try_to_date(d):
        try:
            return d.date()
        except:
            return d

    df[column] = df[column].apply(try_to_date)
    # df[column] = df[column].dt.date


def apply_sex_age(domain, demographics):
    domain.join(
        right=demographics,
        on=["Subject"],
        columns=["S_DM_AGEY_STD", "SEX"])


def apply_common_transforms(domain, demographics, subject_visits):
    # if there are already AGE and SEX, keep the original?
    columns_to_add = [s for s in ["S_DM_AGEY_STD", "SEX"]
                      if s not in domain.data.columns]
    if columns_to_add:
        # print(domain.data.columns)
        # print(demographics.data.columns)
        domain.join(right=demographics,
                    on=["Subject"],  # e-mail 2021-04-16 4:43PM & 5:13PM
                    # on=["subjectId"],
                    columns=columns_to_add)

    # we redo the parse on each pass... :-(
    if "RFSTDTC" not in domain.data.columns:
        domain.join(right=subject_visits,
                    # on=["Subject", "Folder"], #Question in email 2021-04-16 9:20AM
                    # on=["subjectId"], #see e-mail 2021-04-16 4:43 PM
                    on=["Subject"],  # see e-mail 2021-04-16 5:13
                    right_filter={"Folder": "VCHECKIN"},
                    columns={"S_SV_VISDAT_INT": "RFSTDTC"})
        # Inefficient -- we know the format, but it has millis not micros
        make_tc(domain, "RFSTDTC")

    # Note there is no need to rename a column if the column already
    # has that name.
    domain.rename_columns({
        "Subject": "subjectId",  # see e-mail 2021-04-16 4:33PM
        "SiteNumber": "SITEID",
        "Folder": "visitId",
        "FolderName": "VISIT",
        "project": "STUDYID",
        "S_DM_AGEY_STD": "AGE"
    })

    # domain.copy_columns("SITEID", "COUNTRY")

    # domain.map_column("COUNTRY", dictCOUNTRY)

    # this seems to never apply?
    # domain.data["SITEID"] = domain.data["SITEID"].apply(
    #   lambda s: s.split("-",2)[0])


def mapsDY(domain):
    make_tc(domain, "STDTC")
    make_tc(domain, "ENDTC")
    # domain.add_duration("RFSTDTC", "STDTC", "STDY")
    # domain.add_duration("RFSTDTC", "ENDTC", "ENDY")
    # domain.add_duration("STDTC", "ENDTC", "DUR")
    df = domain.data
    df[domain.name + "STDY"] = (df["STDTC"] - df["RFSTDTC"]).dt.days
    df[domain.name + "ENDY"] = (df["ENDTC"] - df["RFSTDTC"]).dt.days
    if domain.name in ["CM", "AE", "MH"]:
        df[domain.name + "DUR"] = (df["ENDTC"] - df["STDTC"]).dt.days


drop_lst = ['projectid', 'studyid', 'environmentName', 'Site', 'siteid', 'StudySiteId',
            'SiteGroup', 'instanceId', 'folderid', 'FolderSeq', 'TargetDays', 'PageRepeatNumber', 'NOW',
            'RecordDate', 'RecordPosition', 'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber'
            ]


def cols_drop(df):
    col_pattern = ['_YYYY', '_STD', '_RAW', '_MM', '_DD', '_CODE', '_CD', '_INT']
    pattern = '|'.join(col_pattern)
    Columns_to_drop = list(df.columns[list(df.columns.str.contains(pattern))])
    # Columns_to_drop2 = list(df.columns[df.columns.str.endswith('_INT')])
    # Columns_to_drop = Columns_to_drop1 + Columns_to_drop2
    return Columns_to_drop


def studyDUR(domain: pd.DataFrame, date_start: str, date_end: str, output_col_name: str):
    domain[date_start] = pd.to_datetime(domain[date_start])

    domain[date_end] = pd.to_datetime(domain[date_end])

    domain.loc[(domain[date_end] - domain[date_start]).dt.days >= 0, output_col_name] = 1 + (
                domain[date_end] - domain[date_start]).dt.days

    domain.loc[(domain[date_end] - domain[date_start]).dt.days < 0, output_col_name] = (
                domain[date_end] - domain[date_start]).dt.days

    domain[output_col_name] = domain[output_col_name].apply(
        lambda x: int(x) if not pd.isnull(x) and not pd.isna(x) else pd.NA)

    return domain

dictlab = {'RCT3': 'GGT',
 'RCT408': 'Creatinine2dp',
 'RCT11': 'SerumGlucose',
 'RCT6': 'UreaNitrogen',
 'RCT5': 'AST',
 'RCT4': 'ALT',
 'RCT4764': 'CreatinClearancePediatSchwartz',
 'RCT15': 'SerumSodium',
 'RCT16': 'SerumPotassium',
 'HMT100': 'MCH',
 'HMT10': 'Monocytes',
 'HMT11': 'Eosinophils',
 'HMT12': 'Basophils',
 'HMT102': 'MCHC',
 'HMT13': 'Platelets',
 'HMT16': 'Lymphocytes',
 'HMT15': 'Neutrophils',
 'HMT17': 'Monocytes',
 'HMT2': 'Hematocrit',
 'HMT18': 'Eosinophils',
 'HMT19': 'Basophils',
 'HMT3': 'RBC',
 'HMT4': 'MCV',
 'HMT40': 'Hemoglobin',
 'HMT71': 'RBCMorphology',
 'HMT8': 'Neutrophils',
 'HMT7': 'WBC',
 'HMT9': 'Lymphocytes',
 'RCT2428': 'Ethanol',
 'UAT2': 'UrSpecificGravity',
 'UAT43': 'UrBlood',
 'UAT3': 'UrpH',
 'UAT49': 'UrProtein',
 'UAT5': 'UrGlucose',
 'UAT53': 'UrUrobilinogen',
 'UAT6': 'UrKetones',
 'UAT7': 'UrBilirubin',
 'RCT2406': 'Amphetamines',
 'RCT2410': 'Barbiturates',
 'RCT2412': 'Cocaine',
 'RCT2422': 'Opiates',
 'RCT2424': 'Cannabinoids'}
