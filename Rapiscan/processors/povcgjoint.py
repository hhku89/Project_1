from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np


def cook(study):
    parts = [
        cook_POVCGVIT(study),
        cook_POIMPVS(study),
        cook_PEVCGVIT(study),
        cook_VS(study)

    ]

    dom = Domain(study, "POVS")
    dom.data = pd.concat([p.data for p in parts], ignore_index=True)
    # dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    # dom.data.columns = dom.data.columns.str.replace("VS_", "VS")

    return dom


def cook_POVCGVIT(study):
    dom = Domain(study, "POVCGVIT")
    dom.data = study.get_raw_domain("povcgvit").data.copy()
    dom.add_column_constant("DOMAIN", "povcgvit")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    dom.data.columns = dom.data.columns.str.replace("VS_", "VS")
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom


def cook_POIMPVS(study):
    dom = Domain(study, "POIMPVS")
    dom.data = study.get_raw_domain("poimpvs").data.copy()
    dom.add_column_constant("DOMAIN", "POIMPVS")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    dom.data.columns = dom.data.columns.str.replace("VS_", "VS")
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom


def cook_PEVCGVIT(study):
    dom = Domain(study, "PEVCGVIT")
    dom.data = study.get_raw_domain("pevcgvit").data.copy()
    dom.add_column_constant("DOMAIN", "PEVCGVIT")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    dom.data.columns = dom.data.columns.str.replace("VS_", "VS")
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom


def cook_VS(study):
    dom = Domain(study, "VS")
    dom.data = study.get_raw_domain("vs").data.copy()
    dom.add_column_constant("DOMAIN", "VS")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.data.columns = dom.data.columns.str.replace("S_VS_", "")
    dom.data.columns = dom.data.columns.str.replace("VS_", "VS")
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom



