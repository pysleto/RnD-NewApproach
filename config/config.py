# Import libraries
import os
from pathlib import Path

import json
import pandas as pd

from config import local
from config import cfg_methods as cfg

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None


def init():
    project_path = Path(os.getcwd())

    (use_case, rnd_path, case_path, config_path) = local.load_local_config(project_path)

    if not Path(project_path).joinpath('config', 'registry.json').exists():
        print('Read Configuration parameters ...')

        # Load config files
        case = cfg.import_my_cases(config_path, use_case, case_path)
        registry = cfg.create_my_registry(config_path, case, project_path, rnd_path)

        with open(Path(project_path).joinpath('config', 'registry.json'), 'w') as file:
            json.dump(registry, file, indent=4)

    print('Load registry ...')

    reg = cfg.load_my_registry(project_path)

    return reg

