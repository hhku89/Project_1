from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np
#
# drop_lst = ['projectid', 'studyid', 'Folder', 'environmentName', 'StudySiteId', 'siteid',
#             'Site', 'instanceId', 'InstanceName', 'InstanceRepeatNumber', 'folderid',
#             'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
#             'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
#             'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber']


def cook(study):
    dom_raw = study.get_raw_domain("cm")
    dom = Domain(study, "CM")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "CM")
    dom.drop_columns("subjectId")
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    # dom.data = dom.data.loc[:, ~dom.data.columns.str.contains('_RAW$|_STD$|_INT$|_YYYY$|_MM$|_DD$')]
    dom.rename_columns({'S_CM_CMSPID': 'CMSPID',
                        'S_CM_CMTRT': 'CMDECOD',
                        'S_CM_CMTRT_ATC': 'CMTRTATC',
                        # 'S_CM_CMTRT_ATC_CODE': 'CMTRT_ATC_CODE',
                        'S_CM_CMTRT_ATC2': 'CMTRTATC2',
                        # 'S_CM_CMTRT_ATC2_CODE': 'CMTRT_ATC2_CODE',
                        'S_CM_CMTRT_ATC3': 'CMTRTATC3',
                        # 'S_CM_CMTRT_ATC3_CODE': 'CMTRT_ATC3_CODE',
                        'S_CM_CMTRT_ATC4': 'CMTRTATC4',
                        # 'S_CM_CMTRT_ATC4_CODE': 'CMTRT_ATC4_CODE',
                        'S_CM_CMTRT_CoderDictName': 'CMCoderDictName',
                        'S_CM_CMTRT_CoderDictVersion': 'CMCoderDictVersion',
                        'S_CM_CMTRT_INGREDIENT': 'CMTRT_INGREDIENT',
                        # 'S_CM_CMTRT_INGREDIENT_CODE': 'CMTRT_INGREDIENT_CODE',
                        'S_CM_CMTRT_PRODUCT': 'CMPRODUCT',
                        # 'S_CM_CMTRT_PRODUCT_CODE': 'CMTRT_PRODUCT_CODE',
                        'S_CM_CMTRT_PRODUCTSYNONYM': 'CMPRODUCTSYNONYM',
                        # 'S_CM_CMTRT_PRODUCTSYNONYM_CODE': 'CMTRT_PRODUCTSYNONYM_CODE',
                        'S_CM_CMINDC': 'CMINDC',
                        'S_CM_CMAENO': 'CMAENO',
                        'S_CM_CMMHNO': 'CMMHNO',
                        'S_CM_CMDSTXT': 'CMDSTXT',
                        'S_CM_CMDOSU': 'CMDOSU',
                        'S_CM_CMDOSFRQ': 'CMDOSFRQ',
                        'S_CM_CMROUTE': 'CMROUTE',
                        'S_CM_CMSTDAT': 'CMSTDTC',
                        'S_CM_CMPRIOR': 'CMPRIOR',
                        'S_CM_CMENDAT': 'CMENDTC',
                        'S_CM_CMONGO': 'CMONGO',
                        'S_CM_CMINDO': 'CMINDO',
                        'S_CM_CMANA': 'CMANA',
                        'S_CM_CMDOSUO': 'CMDOSUO',
                        'S_CM_CMDOSFRQO': 'CMDOSFRQO',
                        'S_CM_CMROUTEO': 'CMROUTEO'})

    common.make_tc(dom, "CMSTDTC")
    common.make_tc(dom, "CMENDTC")

    common.studyDUR(dom.data, "RFSTDTC", "CMSTDTC", "CMSTDY")
    common.studyDUR(dom.data,"RFSTDTC", "CMENDTC", "CMENDY")
    common.studyDUR(dom.data,"CMSTDTC", "CMENDTC", "CMDUR")
    # dom.data["CMSTDY"] = dom.data["CMSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    # dom.data["CMENDY"] = dom.data["CMENDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    # dom.data["CMDUR"] = dom.data["CMDUR"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    dom.filter_rows(lambda r: r["RecordActive"] == '1')

    return dom
