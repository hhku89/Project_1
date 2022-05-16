from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np


# drop_lst=['projectid', 'studyid', 'Folder','environmentName', 'StudySiteId', 'siteid',
#             'Site','instanceId','InstanceName', 'InstanceRepeatNumber', 'folderid',
#             'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
#             'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
#             'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber' ]
def cook(study):
    parts = [
        cook_lbbc(study),
        cook_lbh(study),
        cook_lbi(study),
        cook_lbsc(study),
        cook_lbu(study)

    ]

    dom = Domain(study, "LBS")
    dom.data = pd.concat([p.data for p in parts], ignore_index=True)
    common.make_tc(dom, "LBDAT")
    common.studyDUR(dom.data,"RFSTDTC", "LBDAT", "LBSTDY" )

    return dom


def cook_lbbc(study):
    dom = Domain(study, "lbbc")
    dom.data = study.get_raw_domain("lbbc").data.copy()
    dom.add_column_constant("DOMAIN", "lbbc")
    dom.drop_columns("subjectId")
    dom.rename_columns({'LBTESTBC_STD': 'LBTESTCD'})
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({'LBCATB': 'LBCAT', 'LBTESTBC': 'LBTEST'})
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom


def cook_lbh(study):
    dom = Domain(study, "lbh")
    dom.data = study.get_raw_domain("lbh").data.copy()
    dom.add_column_constant("DOMAIN", "lbh")
    dom.drop_columns("subjectId")
    dom.rename_columns({'LBTESTH_STD': 'LBTESTCD'})
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({'LBCATH': 'LBCAT', 'LBTESTH': 'LBTEST'})
    return dom


def cook_lbi(study):
    dom = Domain(study, "lbi")
    dom.data = study.get_raw_domain("lbi").data.copy()
    dom.add_column_constant("DOMAIN", "lbi")
    dom.drop_columns("subjectId")
    dom.rename_columns({'LBCATCRP_STD': 'LBTESTCD'})
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({'LBCATCRP': 'LBCAT', 'LBTESTI': 'LBTEST'})
    return dom


def cook_lbsc(study):
    dom = Domain(study, "lbsc")
    dom.data = study.get_raw_domain("lbsc").data.copy()
    dom.add_column_constant("DOMAIN", "lbsc")
    dom.drop_columns("subjectId")
    dom.rename_columns({'LBTESTSC_STD': 'LBTESTCD'})
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({'LBCATSC': 'LBCAT', 'LBTESTSC': 'LBTEST'})
    return dom


def cook_lbu(study):
    dom = Domain(study, "lbu")
    dom.data = study.get_raw_domain("lbu").data.copy()
    dom.add_column_constant("DOMAIN", "lbu")
    dom.drop_columns("subjectId")
    dom.rename_columns({'LBTEST_STD': 'LBTESTCD'})
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({'LBCATU': 'LBCAT', 'LBTEST': 'LBTEST'})
    return dom
