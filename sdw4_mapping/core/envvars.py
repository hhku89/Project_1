import os
import logging

class EnvVars:
    """
    Reads default data paths from environment.
    """

    def __init__(self):
        env_vars = list()
        def check_var(var: str):
            if var in os.environ:
                return os.environ.get(var)
            else:
                env_vars.append(var)
                return None

        self.input_folder = check_var('SDW4_DATA_INPUT_DIR')
        self.output_folder = check_var('SDW4_DATA_OUTPUT_DIR')
        self.gen_labels_script_path = check_var('GEN_LABELS_PATH')
        self.cmr_endpoint = check_var('CMR_ENDPOINT')
        self.cmr_loader_script_path = check_var('CMR_LOADER_PATH')
        if self.cmr_loader_script_path is not None:
            self.cmr_pg_username = check_var('CMR_PG_USERNAME')
            check_var('POSTGRES_PW')
            self.cmr_pg_host = check_var('CMR_PG_HOST')
            self.cmr_pg_port = check_var('CMR_PG_PORT')
            self.cmr_pg_db = check_var('CMR_PG_DB')

        if len(env_vars) > 0:
            env_vars_str = ', '.join(env_vars)
            logging.warning(f'{env_vars_str} env var(s) is(are) empty')


envVars = EnvVars()
