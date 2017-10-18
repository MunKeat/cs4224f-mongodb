import os


class Config:
    """
    Use the following to import configuration:
    from config import parameters
    """
    default_parameters = {}
    current_dir_path = os.path.dirname(os.path.realpath(__file__))

    def __init__(self):
        sep = os.path.sep
        config_path = os.path.join(sep, self.current_dir_path, "config.conf")
        data_path = os.path.join(sep, self.current_dir_path, "data")
        xact_path = os.path.join(sep, self.current_dir_path, "xact")
        execfile(config_path, self.default_parameters)
        # Remove unused
        self.default_parameters.pop("__builtins__", None)
        # Set paths
        self.default_parameters['data-path'] = data_path
        self.default_parameters['xact-path'] = xact_path

    def get_default_parameters(self):
        # Please do not assign anything
        return dict(self.default_parameters)


configuration = Config()
parameters = configuration.get_default_parameters()
