# Import libraries
import configparser
import ast
from pathlib import Path


def init():
    use_case = '2018_GLOBAL'  # Prefered kept as capitals as the default section of config.ini has to.
    place = 'home'

    # Set initial parameters
    root_path = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')
    base_path = root_path.joinpath('rnd_new_approach')
    case_path = root_path.joinpath('cases')

    # TODO: adjust to new project structure
    if place == 'office':
        base_path = Path(r'C:\Users\letousi\PycharmProjects\rnd-new_approach')
        data_path = Path(
            r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020')

    print('Read Configuration parameters ...')

    # Load config files
    cases = import_my_cases(use_case, place, root_path, base_path, case_path)
    files = import_my_registry(cases)

    return cases, files


def import_my_cases(use_case, place, root_path, base_path, case_path):
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

    cases.read(case_path.joinpath(r'cases.ini'))

    my_cases = {'use_case': str(use_case),
                'place': str(place),
                'screening_keys': cases.getlist(use_case, 'screening_keys'),
                'regions': cases.getlist(use_case, 'regions'),
                'case_root': case_path.joinpath(cases.get(use_case, 'case_root')),
                'year_first': cases.get(use_case, 'year_first'),
                'year_last': cases.get(use_case, 'year_last'),
                'rnd_limit': cases.getfloat(use_case, 'rnd_limit'),
                'methods': cases.getlist(use_case, 'methods'),
                'company_types': cases.getlist(use_case, 'company_types'),
                'parent_id_files_n': ast.literal_eval(cases.get(use_case, 'parent_id_files_n')),
                'parent_fin_files_n': cases.getint(use_case, 'parent_fin_files_n'),
                'sub_id_files_n': cases.getint(use_case, 'sub_id_files_n'),
                'sub_fin_files_n': cases.getint(use_case, 'sub_fin_files_n'),
                'root': root_path,
                'base': base_path
                }

    return my_cases


def import_my_registry(cases):
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

    config.read(cases['root'].joinpath(r'registry.ini'))

    rnd_outputs = {'id': config.get('RND_OUTPUTS', 'id'),
                   'guo': config.get('RND_OUTPUTS', 'guo'),
                   'bvd9_full': config.get('RND_OUTPUTS', 'bvd9_full'),
                   'bvd9_short': config.get('RND_OUTPUTS', 'bvd9_short'),
                   'fin': config.get('RND_OUTPUTS', 'fin'),
                   'fin_melted': config.get('RND_OUTPUTS', 'fin_melted'),
                   'expo': config.get('RND_OUTPUTS', 'expo'),
                   'rnd': config.get('RND_OUTPUTS', 'rnd')
                   }

    ref_tables = {'country': config.get('REF_TABLES', 'country')}

    # for key, value in rnd_outputs.items():
    #     my_files['rnd_outputs']['parents'][key] = cases['case_root'].joinpath(value + ' - parents.csv')
    #     my_files['rnd_outputs']['subs'][key] = cases['case_root'].joinpath(value + ' - subsidiaries.csv')

    my_reg = {
        'rnd_outputs': {
            'parents': {
                **{k: cases['case_root'].joinpath(v + ' - parents.csv') for k, v in rnd_outputs.items()}
            },
            'subs': {
                **{k: cases['case_root'].joinpath(v + ' - subsidiaries.csv') for k, v in rnd_outputs.items()}
            }
        },
        'ref_tables': {
            **ref_tables
        }
    }

    return my_reg
