from sdw4_mapping.core.envvars import envVars


class Study:
    """
    Contains common info about study for all output domains.
    Inits logger for all study.
    """

    def __init__(self, input_folder: str = envVars.input_folder,
                 output_folder: str = envVars.output_folder,
                 id: str = None):
        self.id = id
        self.output_folder = output_folder
        self.input_folder = input_folder
