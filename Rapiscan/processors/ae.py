from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np


def cook(study):
    dom_raw = study.get_raw_domain("ae")
    dom = Domain(study, "AE")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "AE")
    dom.drop_columns("subjectId")
    drop_lst = ['projectid', 'studyid', 'Folder', 'environmentName', 'StudySiteId', 'siteid',
                'Site', 'instanceId', 'InstanceName', 'InstanceRepeatNumber', 'folderid',
                'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
                'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
                'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber']
    [dom.drop_columns(i) for i in drop_lst]
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    # dom.drop_columns(common.drop_lst)
    # dom.drop_columns(common.cols_drop(dom.data))

    dom.rename_columns({
        "S_AE_AETERM_PT": "AEDECOD",
        # "S_AE_AETERM_PT_CD": "AETERM_PT_CD",
        "S_AE_AETERM_SOC": "AEBODSYS",
        # "S_AE_AETERM_SOC_CD": "AETERM_SOC_CD",
        "S_AE_AESTDAT": "AESTDTC",
        "S_AE_AEENDAT": "AEENDTC",
        'S_AE_AESPID': 'AESPID',
        'S_AE_AETERM': 'AETERM',
        'S_AE_AETERM_CoderDictName': 'AETERM_CoderDictName',
        'S_AE_AETERM_CoderDictVersion': 'AETERM_CoderDictVersion',
        'S_AE_AETERM_HLGT': 'AEHLGT',
        'S_AE_AETERM_HLT': 'AEHLT',
        'S_AE_AESTTIM': 'AESTTIM',
        'S_AE_AESTMUNK': 'AESTMUNK',
        'S_AE_AEENTIM': 'AEENTIM',
        'S_AE_AEETMUNK': 'AEETMUNK',
        'S_AE_AEONGO': 'AEONGO',
        'S_AE_AESEV': 'AESEV',
        'S_AE_AESER': 'AESER',
        'S_AE_AESDTH': 'AESDTH',
        'S_AE_AESLIFE': 'AESLIFE',
        'S_AE_AESHOSP': 'AESHOSP',
        'S_AE_AESDISAB': 'AESDISAB',
        'S_AE_AESCONG': 'AESCONG',
        'S_AE_AESMIE': 'AESMIE',
        'S_AE_AESEROTH': 'AESEROTH',
        'S_AE_AEREL': 'AEREL',
        'S_AE_AEACN': 'AEACN',
        'S_AE_AEDIS': 'AEDIS',
        'S_AE_AEACNOTH': 'AEACNOTH',
        'S_AE_AEOUT': 'AEOUT'

    })

    common.make_tc(dom, "AESTDTC")
    common.make_tc(dom, "AEENDTC")
    common.make_tc(dom, "RFSTDTC")

    common.studyDUR(dom.data,"RFSTDTC", "AESTDTC", "AESTDY")
    common.studyDUR(dom.data,"RFSTDTC", "AEENDTC", "AEENDY")
    common.studyDUR(dom.data,"AESTDTC", "AEENDTC", "AEDUR")

    # dom.data["AESTDY"] = dom.data["AESTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    # dom.data["AEENDY"] = dom.data["AEENDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    # dom.data["AEDUR"] = dom.data["AEDUR"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    # dom.data = dom.data.loc[:, ~dom.data.columns.str.contains('_RAW$|_STD$|_INT$|_YYYY$|_MM$|_DD$|_STD')]
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom
