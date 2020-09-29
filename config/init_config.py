# Import libraries
import os
import ast

import configparser
import json
from shutil import copyfile

from config import local

config = configparser.ConfigParser(
    converters={'list': lambda x: [i.strip() for i in x.split(',')]}
)

print('Read and archive config parameters ...')

config_path = local.project_path.joinpath('config')

config.read(config_path.joinpath(r'config.ini'))

case_path = local.data_path.joinpath(config.get('CASE', 'case'))

copyfile(config_path.joinpath(r'config.ini'), case_path.joinpath(r'#config.ini'))

print('Read local parameters ...')

with open(local.project_path.joinpath('config', 'registry.py'), 'w') as file:

    file.write('import pandas as pd \n')

    file.write('from pathlib import Path \n')

    file.write('pd.options.display.max_columns = None \n')
    file.write('pd.options.display.width = None \n')

    file.write('project_path' + ' = ' + 'Path(r' + repr(str(local.project_path)) + ')' + '\n')
    file.write('data_path' + ' = ' + 'Path(r' + repr(str(local.data_path)) + ')' + '\n')
    file.write('case_path' + ' = ' + 'Path(r' + repr(str(case_path)) + ')' + '\n')
    file.write('rnd_path' + ' = ' + 'Path(r' + repr(str(local.project_path.joinpath('rnd_new_approach'))) + ')' + '\n')
    file.write('config_path' + ' = ' + 'Path(r' + repr(str(config_path)) + ')' + '\n')

    file.write('case' + ' = ' + repr(str(config.get('CASE', 'case'))) + '\n')
    file.write('regions' + ' = ' + str(config.getlist('CASE', 'regions')) + '\n')
    file.write('rnd_limit' + ' = ' + str(config.getfloat('CASE', 'rnd_limit')) + '\n')
    file.write('methods' + ' = ' + str(config.getlist('CASE', 'methods')) + '\n')
    file.write('company_types' + ' = ' + str(config.getlist('CASE', 'company_types')) + '\n')
    file.write('account_conso_type' + ' = ' + str(config.getlist('CASE', 'account_conso_type')) + '\n')
    file.write('account_conso_all' + ' = ' + str(config.getlist('DEFAULT', 'account_conso_all')) + '\n')

    guo_id_path = os.fspath(case_path.joinpath('guos - ' + config.get('DEFAULT', 'out_id') + '.csv'))
    file.write('guo_id_path' + ' = ' + 'Path(r' + repr(str(guo_id_path)) + ')' + '\n')

    parent_id_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_id') + '.csv'))
    parent_bvd9_all_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_bvd9_all') + '.csv'))
    parent_bvd9_select_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_bvd9_select') + '.csv'))
    parent_bvd9_collect_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_bvd9_collect') + '.csv'))
    parent_fin_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_fin') + '.csv'))
    parent_expo_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_expo') + '.csv'))
    parent_rnd_path = os.fspath(case_path.joinpath('parents - ' + config.get('DEFAULT', 'out_rnd') + '.csv'))
    guo_rnd_path = os.fspath(case_path.joinpath('guos - ' + config.get('DEFAULT', 'out_rnd') + '.csv'))


    file.write('parent_id_path' + ' = ' + 'Path(r' + repr(str(parent_id_path)) + ')' + '\n')
    file.write('parent_bvd9_all_path' + ' = ' + 'Path(r' + repr(str(parent_bvd9_all_path)) + ')' + '\n')
    file.write('parent_bvd9_select_path' + ' = ' + 'Path(r' + repr(str(parent_bvd9_select_path)) + ')' + '\n')
    file.write('parent_bvd9_collect_path' + ' = ' + 'Path(r' + repr(str(parent_bvd9_collect_path)) + ')' + '\n')
    file.write('parent_fin_path' + ' = ' + 'Path(r' + repr(str(parent_fin_path)) + ')' + '\n')
    file.write('parent_expo_path' + ' = ' + 'Path(r' + repr(str(parent_expo_path)) + ')' + '\n')
    file.write('parent_rnd_path' + ' = ' + 'Path(r' + repr(str(parent_rnd_path)) + ')' + '\n')
    file.write('guo_rnd_path' + ' = ' + 'Path(r' + repr(str(guo_rnd_path)) + ')' + '\n')
    
    sub_id_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_id') + '.csv'))
    sub_bvd9_all_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_bvd9_all') + '.csv'))
    sub_bvd9_select_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_bvd9_select') + '.csv'))
    sub_bvd9_collect_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_bvd9_collect') + '.csv'))
    sub_fin_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_fin') + '.csv'))
    sub_expo_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_expo') + '.csv'))
    sub_rnd_path = os.fspath(case_path.joinpath('subsidiaries - ' + config.get('DEFAULT', 'out_rnd') + '.csv'))

    file.write('sub_id_path' + ' = ' + 'Path(r' + repr(str(sub_id_path)) + ')' + '\n')
    file.write('sub_bvd9_all_path' + ' = ' + 'Path(r' + repr(str(sub_bvd9_all_path)) + ')' + '\n')
    file.write('sub_bvd9_select_path' + ' = ' + 'Path(r' + repr(str(sub_bvd9_select_path)) + ')' + '\n')
    file.write('sub_bvd9_collect_path' + ' = ' + 'Path(r' + repr(str(sub_bvd9_collect_path)) + ')' + '\n')
    file.write('sub_fin_path' + ' = ' + 'Path(r' + repr(str(sub_fin_path)) + ')' + '\n')
    file.write('sub_expo_path' + ' = ' + 'Path(r' + repr(str(sub_expo_path)) + ')' + '\n')
    file.write('sub_rnd_path' + ' = ' + 'Path(r' + repr(str(sub_rnd_path)) + ')' + '\n')

    id_files_n = ast.literal_eval(config.get('CASE', 'id_files_n'))
    file.write('id_files_n' + ' = ' + str(id_files_n) + '\n')

    fin_files_n = ast.literal_eval(config.get('CASE', 'fin_files_n'))
    file.write('fin_files_n' + ' = ' + str(fin_files_n) + '\n')

    first_year = config.get('CASE', 'first_year')
    last_year = config.get('CASE', 'last_year')
    exp_first_year = config.get('CASE', 'exp_first_year')
    exp_last_year = config.get('CASE', 'exp_last_year')

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

    with open(config_path.joinpath(r'keywords.json'), 'r') as k_file:
        k = json.load(k_file)
        keywords = k['keywords']
        categories = list(keywords.keys())

    file.write('screening_keys' + ' = ' + str(k['screening_keys']) + '\n')
    file.write('categories' + ' = ' + str(categories) + '\n')
    file.write('keywords' + ' = ' + str(keywords) + '\n')

    print('Archive case parameters ...')
