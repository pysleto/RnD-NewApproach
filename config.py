# Import libraries
import sys
from pathlib import Path

import configparser
from tabulate import tabulate

import ast
import json


def init():
    use_case = '2018_GLOBAL'  # Prefered kept as capitals as the default section of config.ini has to.
    place = 'home'

    # Set initial parameters
    project_path = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')
    rnd_path = project_path.joinpath('rnd_new_approach')
    case_path = project_path.joinpath('cases')

    # TODO: adjust to new project structure
    if place == 'office':
        rnd_path = Path(r'C:\Users\letousi\PycharmProjects\rnd-new_approach')
        data_path = Path(
            r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020')

    print('Read Configuration parameters ...')

    # Load config files
    case = import_my_cases(use_case, place, case_path)
    registry = import_my_registry(case, project_path, rnd_path)

    return registry


def import_my_cases(use_case, place, case_path):
    """
    Read cases.ini
    :param use_case: name of the cases.ini section to consider
    :param rnd_path: path (as a string) of folder containing cases.ini
    :param data_path: root path (as a string) for the working folder for corresponding case
    :return: dictionary of configuration parameters
    """
    print('Import cases.ini ...')

    # Import use_case parameters
    cases = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    my_cases_as_strings = {}

    cases.read(case_path.joinpath(r'cases.ini'))

    my_case = {'use_case': str(use_case),
               'place': str(place),
               'screening_keys': cases.getlist(use_case, 'screening_keys'),
               'regions': cases.getlist(use_case, 'regions'),
               'case_root': case_path.joinpath(cases.get(use_case, 'case_root')),
               'year_first': cases.get(use_case, 'year_first'),
               'year_last': cases.get(use_case, 'year_last'),
               'rnd_limit': cases.getfloat(use_case, 'rnd_limit'),
               'methods': cases.getlist(use_case, 'methods'),
               'company_types': cases.getlist(use_case, 'company_types'),
               'parent': {
                   'id_files': ast.literal_eval(cases.get(use_case, 'parent_id_files_n')),
                   'fin_files': cases.getint(use_case, 'parent_fin_files_n'),
               },
               'sub': {
                   'id_files': cases.getint(use_case, 'sub_id_files_n'),
                   'fin_files': cases.getint(use_case, 'sub_fin_files_n')
               }
               }

    return my_case


def import_my_registry(case, project_path, rnd_path):
    """
    Read registry.ini
    :type cases: dictionary of configuration parameters for the considered use case
    :return: dictionary of file paths parameters
    """
    print('Import registry.ini ...')

    # my_files = {
    #     'rnd_outputs': {
    #         'parents': {},
    #         'subs': {}
    #     }
    # }

    # Import use_case parameters
    config = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    config.read(project_path.joinpath(r'registry.ini'))

    rnd_outputs = {'id': config.get('RND_OUTPUTS', 'id'),
                   'guo': config.get('RND_OUTPUTS', 'guo'),
                   'bvd9_full': config.get('RND_OUTPUTS', 'bvd9_full'),
                   'bvd9_short': config.get('RND_OUTPUTS', 'bvd9_short'),
                   'fin': config.get('RND_OUTPUTS', 'fin'),
                   'expo': config.get('RND_OUTPUTS', 'expo'),
                   'rnd': config.get('RND_OUTPUTS', 'rnd')
                   }

    ref_tables = {'country': config.get('REF_TABLES', 'country'),
                  'company': config.get('REF_TABLES', 'company')}

    # for key, value in rnd_outputs.items():
    #     my_files['rnd_outputs']['parents'][key] = cases['case_root'].joinpath(value + ' - parents.csv')
    #     my_files['rnd_outputs']['subs'][key] = cases['case_root'].joinpath(value + ' - subsidiaries.csv')

    my_reg = {
        'project_root': project_path,
        'rnd_root': rnd_path,
        **case,
        'parent': {
            **{k: case['case_root'].joinpath('parents - ' + v + '.csv') for k, v in rnd_outputs.items()},
            **case['parent']
        },
        'sub': {
            **{k: case['case_root'].joinpath('subsidiaries - ' + v + '.csv') for k, v in rnd_outputs.items()},
            **case['sub']
        },
        **ref_tables
    }

    return my_reg
