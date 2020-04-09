# Import libraries
import configparser
import ast
from pathlib import Path


def init():
    use_case = '2018_GLOBAL'
    place = 'home'

    # Set initial parameters
    base_path = Path(r'C:\Users\letousi\PycharmProjects\rnd-new_approach')
    data_path = Path(
        r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020')

    if place == 'home':
        base_path = Path(r'C:\Users\Simon\PycharmProjects\rnd-new_approach')
        data_path = base_path

    print('Read Configuration parameters ...')

    # Load config files
    cases = import_my_cases(use_case, place, base_path, data_path)
    files = import_my_files(cases)

    return cases, files


def import_my_cases(use_case, place, base_path, data_path):
    """
    Read cases.ini
    :param use_case: name of the cases.ini section to consider
    :param base_path: path (as a string) of folder containing cases.ini
    :param data_path: root path (as a string) for the working folder for corresponding case
    :return: dictionary of configuration parameters
    """
    print('Import cases.ini ...')

    # Import use_case parameters
    cases = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    my_cases_as_strings = {}

    cases.read(base_path.joinpath(r'cases.ini'))

    my_cases = {'USE_CASE': str(use_case),
                'PLACE': str(place),
                'SCREENING_KEYS': cases.getlist(use_case, 'SCREENING_KEYS'),
                'REGIONS': cases.getlist(use_case, 'REGIONS'),
                'CASE_ROOT': data_path.joinpath(cases.get(use_case, 'CASE_ROOT')),
                'YEAR_FIRST': cases.get(use_case, 'YEAR_FIRST'),
                'YEAR_LAST': cases.get(use_case, 'YEAR_LAST'),
                'RND_LIMIT': cases.getfloat(use_case, 'RND_LIMIT'),
                'METHODS': cases.getlist(use_case, 'METHODS'),
                'COMPANY_TYPES': cases.getlist(use_case, 'COMPANY_TYPES'),
                'PARENT_ID_FILES_N': ast.literal_eval(cases.get(use_case, 'PARENT_ID_FILES_N')),
                'PARENT_FIN_FILES_N': cases.getint(use_case, 'PARENT_FIN_FILES_N'),
                'SUB_ID_FILES_N': cases.getint(use_case, 'SUB_ID_FILES_N'),
                'SUB_FIN_FILES_N': cases.getint(use_case, 'SUB_FIN_FILES_N'),
                'BASE': base_path
                }

    return my_cases


def import_my_files(cases):
    """
    Read files.ini
    :type cases: dictionary of configuration parameters for the considered use case
    :return: dictionary of file paths parameters
    """
    print('Import files.ini ...')

    my_files = {
        'OUTPUT': {
            'PARENTS': {},
            'SUBS': {}
        },
        'FINAL': {},
        'SOEUR_RND': {}
    }

    # Import use_case parameters
    config = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    config.read(cases['BASE'].joinpath(r'files.ini'))

    my_outputs = {'ID': config.get('OUTPUT', 'ID'),
                  'GUO': config.get('OUTPUT', 'GUO'),
                  'BVD9_FULL': config.get('OUTPUT', 'BVD9_FULL'),
                  'BVD9_SHORT': config.get('OUTPUT', 'BVD9_SHORT'),
                  'FIN': config.get('OUTPUT', 'FIN'),
                  'FIN_MELTED': config.get('OUTPUT', 'FIN_MELTED'),
                  'EXPO': config.get('OUTPUT', 'EXPO'),
                  'RND': config.get('OUTPUT', 'RND')
                  }

    my_finals = {'BY_APPROACH': config.get('FINAL', 'BY_APPROACH')}

    my_soeur_rnds = {'ROOT': config.get('SOEUR_RND', 'ROOT'),
                   'VERSION': config.get('SOEUR_RND', 'VERSION')
                   }

    for key, value in my_outputs.items():
        my_files['OUTPUT']['PARENTS'][key] = cases['CASE_ROOT'].joinpath(value + ' - parents.csv')
        my_files['OUTPUT']['SUBS'][key] = cases['CASE_ROOT'].joinpath(value + ' - subsidiaries.csv')

    for key, value in my_finals.items():
        my_files['FINAL'][key] = cases['CASE_ROOT'].joinpath(value)

    my_files['SOEUR_RND']['VERSION'] = my_soeur_rnds['VERSION']
    my_files['SOEUR_RND']['SOURCE'] = cases['BASE'].joinpath(my_soeur_rnds['ROOT'], 'source', my_soeur_rnds['VERSION'] +
                                                             '.xlsx')
    my_files['SOEUR_RND']['ROOT'] = cases['BASE'].joinpath(my_soeur_rnds['ROOT'])

    return my_files
