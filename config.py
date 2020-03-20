# Import libraries
import configparser
import pandas as pd
import datetime
import json
from pathlib import Path


def init():
    use_case = 'GLOBAL_LC_2018'
    place = 'home'

    # Set initial parameters
    base_path = Path(r'C:\Users\letousi\PycharmProjects\rnd-NewApproach')
    data_path = Path(
        r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020')

    if place == 'home':
        base_path = Path(r'C:\Users\Simon\Documents\PycharmProjects\rnd-NewApproach')
        data_path = base_path

    print('Read Configuration parameters ...')

    # Load config files
    cases, cases_as_strings = import_my_cases(use_case, base_path, data_path)
    files = import_my_files(cases)

    return cases, cases_as_strings, files, use_case, place


def import_my_cases(use_case, base_path, data_path):
    """
    Read cases.ini
    :param use_case: name of the cases.ini section to consider
    :param base: path (as a string) of folder containing cases.ini
    :param data: root path (as a string) for the working folder for corresponding case
    :return: dictionary of configuration parameters
    """
    print('Import cases.ini ...')

    # Import use_case parameters
    cases = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    my_cases_as_strings = {}

    cases.read(base_path.joinpath(r'cases.ini'))

    my_cases = {
        'SCREENING_KEYS': cases.getlist(use_case, 'SCREENING_KEYS'),
        'REGIONS': cases.getlist(use_case, 'REGIONS'),
        'CASE_ROOT': data_path.joinpath(cases.get(use_case, 'CASE_ROOT')),
        'YEAR_LASTAV': cases.getint(use_case, 'YEAR_LASTAV'),
        'SUBS_ID_FILE_N': cases.getint(use_case, 'SUBS_ID_FILE_N'),
        'SUBS_FIN_FILE_N': cases.getint(use_case, 'SUBS_FIN_FILE_N'),
        'MAIN_COMPS_FIN_FILE_N': cases.getint(use_case, 'MAIN_COMPS_FIN_FILE_N'),
        'METHODS': cases.getlist(use_case, 'METHODS'),
        'COMPANY_TYPES': cases.getlist(use_case, 'COMPANY_TYPES'),
        'BASE': base_path
    }

    for key in my_cases.keys():
        my_cases_as_strings[key] = str(my_cases[key])

    return my_cases, my_cases_as_strings


def import_my_files(cases):
    """
    Read files.ini
    :type cases: dictionary of configuration parameters for the considered use case
    :param base: path (as a string) of folder containing files.ini
    :param data: root path (as a string) for the working folder for corresponding case
    :return: dictionary of file paths parameters
    """
    print('Import files.ini ...')

    my_files = {
        'OUTPUT': {},
        'FINAL': {},
        'MAPPING': {}
    }

    # Import use_case parameters
    config = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    config.read(cases['BASE'].joinpath(r'files.ini'))

    my_outputs = {'ID_EXT': config.get('OUTPUT', 'ID_EXT'),
                  'FIN_EXT': config.get('OUTPUT', 'FIN_EXT'),
                  'METHOD_EXT': config.get('OUTPUT', 'METHOD_EXT'),
                  'SCREEN_EXT': config.get('OUTPUT', 'SCREEN_EXT'),
                  'EXPO_EXT': config.get('OUTPUT', 'EXPO_EXT'),
                  'RND_EXT': config.get('OUTPUT', 'RND_EXT')
                  }

    my_finals = {'BENCH': config.get('FINAL', 'BENCH'),
                 'CONSOLIDATED_RND_ESTIMATES': config.get('FINAL', 'CONSOLIDATED_RND_ESTIMATES')}

    my_mappings = {'COUNTRY_SOURCE_PATH': config.get('MAPPING', 'COUNTRY_SOURCE_PATH'),
                   'COUNTRY_REFERENCE_PATH': config.get('MAPPING', 'COUNTRY_REFERENCE_PATH'),
                   'SOEUR_RND_SOURCE_PATH': config.get('MAPPING', 'SOEUR_RND_SOURCE_PATH'),
                   'SOEUR_RND_REFERENCE_PATH': config.get('MAPPING', 'SOEUR_RND_REFERENCE_PATH'),
                   'SOEUR_RND_VERSION': config.get('MAPPING', 'SOEUR_RND_VERSION')
                   }

    for company_type in cases['COMPANY_TYPES']:

        my_files['OUTPUT'][company_type] = {}

        for key, value in my_outputs.items():
            my_files['OUTPUT'][company_type][key] = {}

            my_files['OUTPUT'][company_type][key]['MAIN_COMPS'] = cases['CASE_ROOT'].joinpath(
                str(company_type) + ' - ' + value
            )
            my_files['OUTPUT'][company_type][key]['SUBS'] = cases['CASE_ROOT'].joinpath(
                str(company_type) + ' subsidiaries - ' + value
            )

    for key, value in my_finals.items():
        my_files['FINAL'][key] = cases['CASE_ROOT'].joinpath(value)

    for key, value in my_mappings.items():
        my_files['MAPPING'][key] = cases['CASE_ROOT'].joinpath(value)

    my_files['MAPPING']['SOEUR_RND_VERSION'] = my_mappings['SOEUR_RND_VERSION']

    return my_files
