#!/usr/bin/env python

import os
import settings
from sdw4_mapping import Study
from sdw4_mapping.core.domain import Domain
import inputs

import lib
import lib.common
import processors
import processors.ae
import processors.aereact
import processors.aereact2
import processors.cm
import processors.cmr
import processors.cmrev
import processors.conmeth
import processors.dm
import processors.ds
import processors.dseos
import processors.ecgd
import processors.eg
import processors.egfr
import processors.ex
import processors.exg
import processors.flu
import processors.fpe
import processors.ie
import processors.lb
import processors.mh
import processors.mhyn
import processors.pe
import processors.pevcg
import processors.poim
import processors.povcg
import processors.povcgjoint
import processors.sae
import processors.su
import processors.sub
import processors.sus
import processors.sv
import processors.vs
# import processors.lbs


class Rapiscan(Study):
    def __init__(self,
                 id="CCD-050000-01",
                 input_folder=settings.input_folder,
                 preprocessed_folder=os.path.join(  # e.g., +CreatedDate
                     settings.input_folder,
                     "output"),
                 archive_folder=os.path.join(
                     settings.input_folder,
                     "archive"),
                 output_folder=settings.output_folder):
        super().__init__(id=id,
                         input_folder=preprocessed_folder,
                         output_folder=output_folder)
        self.raw_input_folder = input_folder
        self.archive_folder = archive_folder
        self.raw_domain_reader = inputs.InputReader(self)
        self.demographics2 = None

    def get_raw_domain(self, domain_name):
        return self.raw_domain_reader.get(domain_name)

    def apply_common_transforms(self, domain):
        demographics = self.get_raw_domain("dm")
        subject_visits = self.get_raw_domain("sv")
        lib.common.apply_common_transforms(
            domain=domain,
            demographics=demographics,
            subject_visits=subject_visits)

    def get_demographics2(self):
        if self.demographics2 is None:
            demographics = self.get_raw_domain("dm")
            dm2 = Domain(self, "DM2")
            dm2.data = demographics.data.copy()
            dm2.data = dm2.data[['Subject', 'S_DM_SEX', 'S_DM_AGE']]
            dm2.rename_columns({'S_DM_SEX': 'SEX', 'S_DM_AGE': 'AGE'})
            dm2.data = dm2.data[['Subject', 'SEX', 'AGE']]
            dm2.data = dm2.data.drop_duplicates()
            self.demographics2 = dm2
        return self.demographics2

    def apply_sex_age(self, domain):
        demographics = self.get_demographics2()
        lib.common.apply_sex_age(domain, demographics)

    def cook_ae(self):
        return processors.ae.cook(self)

    def cook_aereact(self):
        return processors.aereact.cook(self)

    def cook_aereact2(self):
        return processors.aereact2.cook(self)

    def cook_cm(self):
        return processors.cm.cook(self)

    def cook_cmr(self):
        return processors.cmr.cook(self)

    def cook_cmrev(self):
        return processors.cmrev.cook(self)

    def cook_conmeth(self):
        return processors.conmeth.cook(self)

    def cook_dm(self):
        return processors.dm.cook(self)

    def cook_ds(self):
        return processors.ds.cook(self)

    def cook_dseos(self):
        return processors.dseos.cook(self)

    def cook_ecgd(self):
        return processors.ecgd.cook(self)

    def cook_eg(self):
        return processors.eg.cook(self)

    def cook_egfr(self):
        return processors.egfr.cook(self)

    def cook_ex(self):
        return processors.ex.cook(self)

    def cook_exg(self):
        return processors.exg.cook(self)

    def cook_flu(self):
        return processors.flu.cook(self)

    def cook_fpe(self):
        return processors.fpe.cook(self)

    def cook_ie(self):
        return processors.ie.cook(self)

    def cook_lb(self):
        return processors.lb.cook(self)

    def cook_mh(self):
        return processors.mh.cook(self)

    def cook_mhyn(self):
        return processors.mhyn.cook(self)

    def cook_pe(self):
        return processors.pe.cook(self)

    def cook_pevcg(self):
        return processors.pevcg.cook(self)

    def cook_poim(self):
        return processors.poim.cook(self)

    def cook_povcg(self):
        return processors.povcg.cook(self)

    def cook_povcgjoint(self):
        return processors.povcgjoint.cook(self)

    def cook_sae(self):
        return processors.sae.cook(self)

    def cook_su(self):
        return processors.su.cook(self)

    def cook_sub(self):
        return processors.sub.cook(self)

    def cook_sus(self):
        return processors.sus.cook(self)

    def cook_sv(self):
        return processors.sv.cook(self)

    def cook_vs(self):
        return processors.vs.cook(self)

    # def cook_lbs(self):
    #     return processors.lbs.cook(self)
