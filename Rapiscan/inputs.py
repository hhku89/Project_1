#!/usr/bin/env python

from sdw4_mapping import Domain
import os


class InputReader:
    def __init__(self, study):
        self.study = study
        self.cache = {}

    def get(self, domain_name):
        # Take advantage that all files are ae.CSV, etc.
        domain = self.cache.get(domain_name)
        if domain:
            return domain

        filename = f"{domain_name}.csv"
        filename = os.path.join(self.study.input_folder, filename)

        try:
            domain = Domain(study=self.study, name=domain_name,
                            data_path=filename)
            self.cache[domain_name] = domain
            return domain
        except Exception as e:
            raise Exception(f"Failed to read {filename}: {e}", e)
