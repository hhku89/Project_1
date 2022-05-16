from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np

drop_lst=['projectid', 'studyid', 'Folder','environmentName', 'StudySiteId', 'siteid',
        'Site','instanceId','InstanceName', 'InstanceRepeatNumber', 'folderid',
        'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
        'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
        'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber' ]

col_pattern = ["S_VS_", "VS_"]
pattern = "|" .join(col_pattern)

def cook(study):
    dom_raw = study.get_raw_domain("vs")
    dom = Domain(study, "VS")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "VS")
    dom.drop_columns("subjectId")
    dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    dom.data.columns = dom.data.columns.str.replace("VS_", "VS")
    # [dom.drop_columns(i) for i in drop_lst]
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    common.make_tc(dom, 'VSDAT')
    common.studyDUR(dom.data, "RFSTDTC", "VSDAT", "VSSTDY")
    dom.data = pd.melt(dom.data, id_vars=['STUDYID', 'subjectId', 'SITEID', 'InstanceName',
       'InstanceRepeatNumber', 'visitId', 'VISIT', 'DataPageId',
       'DataPageName', 'RecordId', 'RecordActive', 'VSPERF', 'VSDAT', 'VSSTDY',
       'VSTIM', 'VSPOS', 'NDRSN', 'VSLOC', 'VSOVRINT', 'DateCreated', 'DOMAIN', 'AGE',
       'SEX', 'RFSTDTC'], value_vars=['VSSYSBP', 'VSDIABP', 'VSHR', 'VSRESP','VSOXY'])
    dom.rename_columns({'VSDAT': 'VSSTDTC', 'variable': 'VSTESTCD', 'value': 'VSSTRESN' })
    dom.filter_rows(lambda r: r["RecordActive"] == '1')

    return dom

