import configparser


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


def import_my_files(cases, company_type, base_path, data_path):
    """
    Read files.ini
    :type cases: dictionary of configuration parameters for the considered use case
    :param base: path (as a string) of folder containing files.ini
    :param data: root path (as a string) for the working folder for corresponding case
    :return: dictionary of file paths parameters
    """
    print('Import files.ini ...')

    my_files = {
        'MAIN_COMPS': {},
        'SUBS': {}
    }
    my_extensions = {}

    # Import use_case parameters
    file_extensions = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    file_extensions.read(base_path.joinpath(r'files.ini'))

    my_extensions = {'ID_EXT': file_extensions.get('OUTPUT', 'ID_EXT'),
                     'FIN_EXT': file_extensions.get('OUTPUT', 'FIN_EXT'),
                     'METHOD_EXT': file_extensions.get('OUTPUT', 'METHOD_EXT'),
                     'SCREEN_EXT': file_extensions.get('OUTPUT', 'SCREEN_EXT'),
                     'EXPO_EXT': file_extensions.get('OUTPUT', 'EXPO_EXT'),
                     'RND_EXT': file_extensions.get('OUTPUT', 'RND_EXT')
                     }

    for key, values in my_extensions.items():
        my_files['MAIN_COMPS'][key] = cases['CASE_ROOT'].joinpath(str(company_type) + ' - ' + my_extensions[key])
        my_files['SUBS'][key] = cases['CASE_ROOT'].joinpath(str(company_type) + ' subsidiaries - ' + my_extensions[key])

    return my_files
