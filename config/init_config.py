# Import libraries
import os
import ast

import configparser
import json
import pandas as pd

from config import local

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None

config = configparser.ConfigParser(
    converters={'list': lambda x: [i.strip() for i in x.split(',')]}
)

print('Read local parameters ...')

with open(local.project_path.joinpath('config', 'registry.py'), 'w') as file:

    with open(local.project_path.joinpath('config', 'local.py'), 'r') as local_file:
        for line in local_file:
            file.write(line)
        file.write('\n')

    print('Read config parameters ...')

    config.read(local.config_path.joinpath(r'config.ini'))

    file.write('regions' + ' = ' + str(config.getlist(local.use_case, 'regions')) + '\n')
    file.write('rnd_limit' + ' = ' + str(config.getfloat(local.use_case, 'rnd_limit')) + '\n')
    file.write('methods' + ' = ' + str(config.getlist(local.use_case, 'methods')) + '\n')
    file.write('company_types' + ' = ' + str(config.getlist(local.use_case, 'company_types')) + '\n')

    parent_id_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_id') + '.csv'))
    parent_guo_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_guo') + '.csv'))
    parent_bvd9_full_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_bvd9_full') + '.csv'))
    parent_bvd9_short_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_bvd9_short') + '.csv'))
    parent_fin_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_fin') + '.csv'))
    parent_expo_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_expo') + '.csv'))
    parent_rnd_path = os.fspath(local.case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_rnd') + '.csv'))
    parent_id_files_n = ast.literal_eval(config.get(local.use_case, 'parent_id_files_n'))
    parent_fin_files_n = config.getint(local.use_case, 'parent_fin_files_n')

    file.write('parent_id_path' + ' = ' + 'Path(r' + repr(str(parent_id_path)) + ')' + '\n')
    file.write('parent_guo_path' + ' = ' + 'Path(r' + repr(str(parent_guo_path)) + ')' + '\n')
    file.write('parent_bvd9_full_path' + ' = ' + 'Path(r' + repr(str(parent_bvd9_full_path)) + ')' + '\n')
    file.write('parent_bvd9_short_path' + ' = ' + 'Path(r' + repr(str(parent_bvd9_short_path)) + ')' + '\n')
    file.write('parent_fin_path' + ' = ' + 'Path(r' + repr(str(parent_fin_path)) + ')' + '\n')
    file.write('parent_expo_path' + ' = ' + 'Path(r' + repr(str(parent_expo_path)) + ')' + '\n')
    file.write('parent_rnd_path' + ' = ' + 'Path(r' + repr(str(parent_rnd_path)) + ')' + '\n')
    file.write('parent_id_files_n' + ' = ' + str(parent_id_files_n) + '\n')
    file.write('parent_fin_files_n' + ' = ' + str(parent_fin_files_n) + '\n')
    
    sub_id_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_id') + '.csv'))
    sub_guo_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_guo') + '.csv'))
    sub_bvd9_full_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_bvd9_full') + '.csv'))
    sub_bvd9_short_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_bvd9_short') + '.csv'))
    sub_fin_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_fin') + '.csv'))
    sub_expo_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_expo') + '.csv'))
    sub_rnd_path = os.fspath(local.case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_rnd') + '.csv'))
    sub_id_files_n = ast.literal_eval(config.get(local.use_case, 'sub_id_files_n'))
    sub_fin_files_n = config.getint(local.use_case, 'sub_fin_files_n')

    file.write('sub_id_path' + ' = ' + 'Path(r' + repr(str(sub_id_path)) + ')' + '\n')
    file.write('sub_guo_path' + ' = ' + 'Path(r' + repr(str(sub_guo_path)) + ')' + '\n')
    file.write('sub_bvd9_full_path' + ' = ' + 'Path(r' + repr(str(sub_bvd9_full_path)) + ')' + '\n')
    file.write('sub_bvd9_short_path' + ' = ' + 'Path(r' + repr(str(sub_bvd9_short_path)) + ')' + '\n')
    file.write('sub_fin_path' + ' = ' + 'Path(r' + repr(str(sub_fin_path)) + ')' + '\n')
    file.write('sub_expo_path' + ' = ' + 'Path(r' + repr(str(sub_expo_path)) + ')' + '\n')
    file.write('sub_rnd_path' + ' = ' + 'Path(r' + repr(str(sub_rnd_path)) + ')' + '\n')
    file.write('sub_id_files_n' + ' = ' + str(sub_id_files_n) + '\n')
    file.write('sub_fin_files_n' + ' = ' + str(sub_fin_files_n) + '\n')

    first_year = config.get(local.use_case, 'first_year')
    last_year = config.get(local.use_case, 'last_year')
    exp_first_year = config.get(local.use_case, 'exp_first_year')
    exp_last_year = config.get(local.use_case, 'exp_last_year')

    file.write('first_year' + ' = ' + str(first_year) + '\n')
    file.write('last_year' + ' = ' + str(last_year) + '\n')
    file.write('exp_first_year' + ' = ' + str(exp_first_year) + '\n')
    file.write('exp_last_year' + ' = ' + str(exp_last_year) + '\n')

    LY = repr(str(last_year[-2:]))
    rnd_ys =['rnd_y' + str(YY) for YY in range(int(first_year[-2:]), int(last_year[-2:]) + 1)]
    oprev_ys = ['op_revenue_y' + str(YY) for YY in range(int(first_year[-2:]), int(last_year[-2:]) + 1)]
    oprev_ys_for_exp = ['op_revenue_y' + str(YY) for YY in range(int(exp_first_year[-2:]), int(exp_last_year[-2:]) + 1)]

    file.write('LY' + ' = ' + str(LY) + '\n')
    file.write('rnd_ys' + ' = ' + str(rnd_ys) + '\n')
    file.write('oprev_ys' + ' = ' + str(oprev_ys) + '\n')
    file.write('oprev_ys_for_exp' + ' = ' + str(oprev_ys_for_exp) + '\n')

    print('Read keywords parameters ...')

    with open(local.config_path.joinpath(r'keywords.json'), 'r') as k_file:
        k = json.load(k_file)
        keywords = k['keywords']
        categories = list(keywords.keys())

    file.write('screening_keys' + ' = ' + str(k['screening_keys']) + '\n')
    file.write('categories' + ' = ' + str(categories) + '\n')
    file.write('keywords' + ' = ' + str(keywords) + '\n')
