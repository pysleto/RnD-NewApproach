# Import libraries
from pathlib import Path

import pandas as pd
import numpy as np

import json
from tabulate import tabulate

from data_input import file_loader as load

import init_config as cfg

# Load config files
reg = cfg.load_my_registry()

# Load keywords for activity screening
with open(reg['rnd_root'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

categories = list(keywords.keys())

rnd_cluster_cats = [cat for cat in categories if cat not in ['generation', 'rnd']]

# Import mapping tables
# country_ref = pd.read_csv(reg['country'], error_bad_lines=False, encoding='UTF-8')
country_ref = pd.read_csv(reg['country'])

def load_parent_ids():
    """
    Load identification data for parent companies
    """

    parent_ids = df_cache = pd.DataFrame()

    report = {}

    print('Read parent company ids from input tables')

    for company_type in reg['company_types']:
        print('... ' + str(company_type))

        df = load.parent_ids_from_orbis_xls(
            reg['case_root'].joinpath(r'input/parent_ids'),
            reg['parent']['id_files'][company_type],
            company_type
        )

        df['is_' + str(company_type)] = True
        df_cache[company_type] = df['bvd9']

        # Consolidate subsidiaries financials
        parent_ids = parent_ids.append(df)

    # Drop #N/A and duplicates
    parent_ids = parent_ids.dropna(subset=['bvd9'], how='all')
    parent_ids = parent_ids.drop_duplicates(subset='bvd9', keep='first')

    # Flag source company type in consolidated data set
    for company_type in reg['company_types']:
        parent_ids['is_' + str(company_type)] = False
        parent_ids.loc[parent_ids['bvd9'].isin(df_cache[company_type]), 'is_' + str(company_type)] = True

    # Define column ids
    id_columns = ['bvd9', 'company_name', 'bvd_id', 'legal_entity_id', 'guo_bvd9'] + \
                 ['is_' + str(company_type) for company_type in reg['company_types']] + \
                 ['NACE_4Dcode', 'NACE_desc', 'subs_n'] + \
                 ['country_2DID_iso']

    print('Merge with country_ref ...')

    # Merge with country_ref for allocation to world player categories
    id_merge = pd.merge(
        parent_ids[id_columns],
        country_ref[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    guo_columns = ['guo_bvd9', 'guo_type', 'guo_name', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso']

    guo_merge = pd.merge(
        parent_ids[guo_columns],
        country_ref[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='guo_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).rename(columns={'country_3DID_iso': 'guo_country_3DID_iso', 'world_player': 'guo_world_player'})

    guo_merge.drop_duplicates(['guo_bvd9'], keep='first', inplace=True)

    print('Save parent company ids output files ...')

    # Save output table of selected parent companies
    id_merge.to_csv(reg['parent']['id'],
                    columns=id_columns + ['country_3DID_iso', 'world_player'],
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    guo_merge.to_csv(reg['parent']['guo'],
                     columns=guo_columns + ['guo_country_3DID_iso', 'guo_world_player'],
                     float_format='%.10f',
                     index=False,
                     na_rep='#N/A'
                     )

    return id_merge, guo_merge
    # return report, id_merge, guo_merge


def load_parent_fins():
    """
    Load financials data for parent companies
    """
    parent_fins = pd.DataFrame()
    report = {}

    print('Read parent companies financials input files')

    parent_fins = load.parent_fins_from_orbis_xls(
        reg['case_root'].joinpath(r'input/parent_fins'),
        reg['parent']['fin_files'],
        reg['oprev_ys'],
        reg['rnd_ys'],
        reg['LY']
    )

    parent_fins = parent_fins.dropna(subset=reg['rnd_ys'], how='all')

    for cols in reg['rnd_ys']:
        parent_fins[parent_fins[cols] < 0] = 0

    parent_fins['rnd_mean'] = parent_fins[reg['rnd_ys']].mean(axis=1, skipna=True)

    parent_fin_cols = ['bvd9', 'Emp_number_y' + reg['LY'], 'sales_y' + reg['LY'],
                       'rnd_mean'] + reg['rnd_ys'][::-1] + reg['oprev_ys'][::-1]

    # parent_fins['Emp_number_y' + reg['LY']] = parent_fins['Emp_number_y' + reg['LY']].astype(int)

    # Save it as csv
    parent_fins.to_csv(reg['parent']['fin'],
                       columns=parent_fin_cols,
                       float_format='%.10f',
                       index=False,
                       na_rep='#N/A'
                       )

    # melted = parent_fins.melt(
    #     id_vars=['bvd9'],
    #     value_vars=['Emp_number_y' + reg['LY'], 'operating_revenue_y' + reg['LY'], 'sales_y' + reg['LY']] + reg['rnd_ys'][::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg['parent']['fin_melted'],
    #               columns=['bvd9', 'year', 'type', 'value'],
    #               float_format='%.10f',
    #               index=False,
    #               na_rep='#N/A'
    #               )

    return parent_fins
    # return report, parent_fins


def select_parent_ids_with_rnd(
        parent_fins
):
    # Identify the top companies that constitute 99% of the R&D expenses
    start = 0.0
    count = 0

    print('Select parent companies representing ' + str(reg['rnd_limit']) + ' of total RnD')

    parent_fins.sort_values(by='rnd_mean', ascending=False, na_position='last')

    while start < reg['rnd_limit'] * parent_fins['rnd_mean'].sum():
        count += 1
        start = parent_fins.nlargest(count, ['rnd_mean'])['rnd_mean'].sum()

    selected_parent_ids = parent_fins.nlargest(count, ['rnd_mean'])

    selected_parent_ids.drop_duplicates(subset=['bvd9'], keep='first')

    return selected_parent_ids


def load_sub_ids():
    """
    Consolidate a unique list of subsidiaries
    """
    # Initialize DF
    sub_ids = pd.DataFrame()
    report = {}

    print('Read subsidiary identification input tables')

    sub_ids = load.sub_ids_from_orbis_xls(
        reg['case_root'].joinpath(r'input/sub_ids'),
        reg['sub']['id_files']
    )

    # Drop not bvd identified subsidiaries and (group,subs) duplicates
    sub_ids = sub_ids.dropna(subset=['bvd9', 'sub_bvd9']).drop_duplicates(['bvd9', 'sub_bvd9'], keep='first')

    report['Claimed by parent companies'] = {'selected_bvd9': sub_ids['bvd9'].nunique(),
                                             'sub_bvd9_in_selected_bvd9': sub_ids['sub_bvd9'].count().sum(),
                                             'unique_sub_bvd9': sub_ids['sub_bvd9'].nunique()
                                             }

    print('Merge with country_ref ...')

    # Merge with country_ref for allocation to world player categories
    id_merge = pd.merge(
        sub_ids[['company_name', 'bvd9', 'sub_company_name', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id',
                 'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl']],
        country_ref[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).rename(columns={'country_3DID_iso': 'sub_country_3DID_iso', 'world_player': 'sub_world_player'})

    print('Save subsidiaries output file ...')

    sub_id_cols = ['sub_bvd9', 'bvd9', 'sub_company_name', 'sub_bvd_id',
                   'sub_legal_entity_id', 'sub_lvl', 'sub_country_2DID_iso', 'sub_country_3DID_iso', 'sub_world_player']

    # TODO: Check for subsidiaries that have the same name but several corresponding bvd9 ids
    # Save it as csv
    id_merge.to_csv(reg['sub']['id'],
                    columns=sub_id_cols,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    return sub_ids
    # return report, sub_ids


def load_sub_fins():
    """
    Loads financials for subsidiaries
    """
    sub_fins = pd.DataFrame()
    report = {}


    print('Read subsidiaries financials input tables')

    sub_fins = load.sub_fins_from_orbis_xls(
        reg['case_root'].joinpath(r'input/sub_fins'),
        reg['sub']['fin_files'],
        reg['oprev_ys'],
        reg['rnd_ys']
    )

    # sub_fins = sub_fins[sub_fins['sub_bvd9'].isin(select_subs['sub_bvd9'])]

    sub_fins = sub_fins.drop_duplicates('sub_bvd9')

    for cols in reg['rnd_ys']:
        sub_fins[sub_fins[cols] < 0] = 0

    sub_fins_w_fin = sub_fins.dropna(subset=reg['oprev_ys'], how='all')

    report['Returned by ORBIS'] = {'sub_bvd9_in_selected_bvd9': sub_fins['sub_bvd9'].count().sum(),
                                   'unique_sub_bvd9': sub_fins['sub_bvd9'].nunique(),
                                   'unique_has_fin': sub_fins_w_fin['sub_bvd9'].nunique(),
                                   }

    # # Merging subsidiary country_ref for allocation to world player categories and countries
    # merged = pd.merge(
    #     sub_fins_w_fin, country_ref[['country_2DID_iso', 'country_3DID_iso', 'region', 'world_player']],
    #     left_on='country_iso', right_on='country_2DID_iso',
    #     how='left',
    #     suffixes=(False, False)
    # )

    sub_fins_cols = ['sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'] + \
                    reg['oprev_ys'][::-1] + reg['rnd_ys'][::-1]

    # Save it as csv
    sub_fins.to_csv(reg['sub']['fin'],
                    columns=sub_fins_cols,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    # melted = sub_fins.melt(
    #     id_vars=['sub_company_name', 'sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'],
    #     value_vars=reg['oprev_ys'][::-1] + reg['rnd_ys'][::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg['sub']['fin_melted'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    return sub_fins
    # return report, sub_fins


def screen_sub_ids_for_method(
        parent_ids,
        sub_ids
):
    """
    Add bolean masks for the implementation of different rnd calculation method
    keep_all: Keep all parent companies and all subsidiaries
    keep_comps: Keep all parent companies and exclude subsidiaries that are parent companies from subsidiaries list
    keep_subs: Exclude parent companies that are a subsidiary from companies list and keep all subsidiaries

    """
    report = {}

    print('Screen subsidiaries for method flags')

    # Flag parent companies that are a subsidiary of another parent company and vice versa
    # sub_ids['is_comp_a_sub'] = sub_ids['is_comp_a_sub'].isin(sub_ids['sub_bvd9'])
    # sub_ids['is_sub_a_comp'] = sub_ids['sub_bvd9'].isin(sub_ids['bvd9'])
    # sub_ids['has_fin'] = sub_ids['sub_bvd9'].isin(sub_fins['sub_bvd9'])

    # Flag subsidiaries that are subsidiaries of multiple parent companies
    sub_ids['is_sub_a_duplicate'] = sub_ids['sub_bvd9'].duplicated(keep=False)

    sub_ids['keep_all'] = True

    # sub_ids.loc[~sub_ids['bvd9'].isin(sub_ids['sub_bvd9']), 'keep_subs'] = True
    # sub_ids.loc[sub_ids['keep_subs'] != True, 'keep_subs'] = False
    # sub_ids.loc[~sub_ids['sub_bvd9'].isin(parent_ids['bvd9']), 'keep_comps'] = True
    # sub_ids.loc[sub_ids['keep_comps'] != True, 'keep_comps'] = False

    sub_ids['keep_subs'] = ~sub_ids['bvd9'].isin(sub_ids['sub_bvd9'])
    sub_ids['keep_comps'] = ~sub_ids['sub_bvd9'].isin(parent_ids['bvd9'])

    for method in reg['methods']:
        print('Flag strategy: ' + str(method))

        report['From ORBIS with applied method: ' + str(method)] = {
            'selected_bvd9': sub_ids['bvd9'][sub_ids[method] == True].nunique(),
            'unique_sub_bvd9': sub_ids['sub_bvd9'][sub_ids[method] == True].nunique()
            # 'unique_has_fin': sub_ids['sub_bvd9'][
            #     (sub_ids[method] == True) & (sub_ids['has_fin'] == True)].nunique()
        }

    print('Save output files with filters ...')

    sub_ids_cols = ['sub_bvd9', 'bvd9', 'sub_company_name', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_country_2DID_iso',
                    'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl', 'keep_all', 'keep_comps', 'keep_subs']

    # Save it as csv
    sub_ids.to_csv(reg['sub']['id'],
                   columns=sub_ids_cols,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

    return sub_ids
    # return report, sub_ids

def screen_sub_fins_for_keywords(
        sub_fins
):
    print('Screen subsidiary activity for keywords')

    categories = list(keywords.keys())

    report = {}

    for category in categories:

        sub_fins[category] = False

        for keyword in keywords[category]:
            sub_fins[category] |= sub_fins['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['products&services_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['full_overview_desc'].str.contains(keyword, case=False, regex=False)

    # screen_subs = sub_fins.loc[:, ['sub_company_name', 'sub_bvd9', 'sub_bvd_id'] + categories]

    sub_fins['sub_turnover_sum'] = sub_fins.loc[:, reg['oprev_ys_for_exp']].sum(axis=1)

    sub_fins['keyword_mask'] = list(
        map(bool, sub_fins[[cat for cat in categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    sub_fins['sub_turnover_sum_masked'] = sub_fins['sub_turnover_sum'].mask(~sub_fins['keyword_mask'])

    report['Returned by ORBIS'] = {
        'unique_is_matching_a_keyword': sub_fins['sub_bvd9'][sub_fins['keyword_mask'] == True].nunique()
    }

    sub_fins_cols = ['sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'] + \
                    reg['oprev_ys_for_exp'][::-1]

    # Save it as csv
    sub_fins.to_csv(reg['sub']['fin'],
                    columns=sub_fins_cols +
                            ['sub_turnover_sum', 'sub_turnover_sum_masked', 'keyword_mask'] +
                            [cat for cat in categories],
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    return sub_fins
    # return report, sub_fins

def compute_exposure(
        selected_sub_ids,
        sub_fins
):
    sub_exposure_conso = pd.DataFrame()
    parent_exposure_conso = pd.DataFrame()
    report_keyword_match = {}
    report_exposure = {'at_subsidiary_level': {}, 'at_parent_level': {}}

    print('Compute exposure for strategy:')

    for method in reg['methods']:
        print('... ' + str(method))
        sub_exposure = pd.DataFrame()

        # Merging selected subsidiaries by method with masked turnover and turnover
        sub_exposure = pd.merge(
            selected_sub_ids[selected_sub_ids[method] == True], sub_fins,
            left_on=['sub_bvd9'], right_on=['sub_bvd9'],
            how='left'
        )

        # Calculating group exposure
        parent_exposure = sub_exposure[
            ['bvd9', 'sub_turnover_sum_masked', 'sub_turnover_sum']
        ].groupby(['bvd9']).sum().rename(
            columns={'sub_turnover_sum': 'total_sub_turnover_sum_in_parent',
                     'sub_turnover_sum_masked': 'total_sub_turnover_sum_masked_in_parent'}
        )

        parent_exposure['parent_exposure'] = parent_exposure['total_sub_turnover_sum_masked_in_parent'] / \
                                             parent_exposure['total_sub_turnover_sum_in_parent']

        parent_exposure['method'] = str(method)

        parent_exposure.reset_index(inplace=True)

        parent_exposure_conso = parent_exposure_conso.append(parent_exposure)

        # Calculating subsidiary level exposure
        sub_exposure = pd.merge(
            sub_exposure, parent_exposure[
                ['bvd9', 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
                 'parent_exposure']],
            left_on='bvd9', right_on='bvd9',
            how='left'
        )

        sub_exposure['sub_exposure'] = sub_exposure['sub_turnover_sum_masked'] / sub_exposure[
            'total_sub_turnover_sum_in_parent']

        sub_exposure['method'] = str(method)

        sub_exposure.dropna(subset=['parent_exposure', 'sub_turnover_sum'], inplace=True)

        report_keyword_match['From ORBIS with applied method: ' + str(method)] = {
            'sub_bvd9_in_selected_bvd9': selected_sub_ids['sub_bvd9'][selected_sub_ids[method] == True].count().sum(),
            'unique_is_matching_a_keyword': sub_exposure['sub_bvd9'][sub_exposure['keyword_mask'] == True].nunique()
        }

        report_exposure['at_parent_level'].update({
            'With method: ' + str(method): {
                'Total_exposure': parent_exposure['parent_exposure'].sum()
            }
        })

        report_exposure['at_subsidiary_level'].update({
            'With method: ' + str(method): {
                'Total_exposure': sub_exposure['sub_exposure'].sum()
            }
        })

        sub_exposure_conso = sub_exposure_conso.append(sub_exposure)

    parent_exposure_cols = ['bvd9', 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
                            'parent_exposure', 'method']

    # Save output tables
    parent_exposure_conso.to_csv(reg['parent']['expo'],
                                 columns=parent_exposure_cols,
                                 float_format='%.10f',
                                 index=False,
                                 na_rep='#N/A'
                                 )

    sub_exposure_conso.to_csv(reg['sub']['expo'],
                              columns=['sub_bvd9',
                                       'sub_turnover_sum',
                                       'sub_turnover_sum_masked',
                                       'sub_exposure'] + parent_exposure_cols,
                              float_format='%.10f',
                              index=False,
                              na_rep='#N/A'
                              )

    return parent_exposure_conso, sub_exposure_conso
    # return report_keyword_match, report_exposure, parent_exposure_conso, sub_exposure_conso

def compute_parent_rnd(
        parent_exposure,
        parent_fins
):
    print('Compute parent level rnd')

    parent_rnd_conso = pd.DataFrame()

    report_parent_rnd = {}

    parent_rnd = pd.merge(parent_exposure, parent_fins,
                          left_on='bvd9', right_on='bvd9',
                          how='left'
                          )

    for method in reg['methods']:
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating group level rnd
        rnd_melt = parent_rnd_method.melt(
            id_vars=['bvd9', 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
                     'parent_exposure'],
            value_vars=reg['rnd_ys'],
            var_name='rnd_label', value_name='parent_rnd')

        rnd_melt['year'] = [int('20' + s[-2:]) for s in rnd_melt['rnd_label']]

        oprev_melt = parent_rnd_method.melt(
            id_vars=['bvd9'],
            value_vars=reg['oprev_ys'],
            var_name='oprev_label', value_name='parent_oprev')

        oprev_melt['year'] = [int('20' + s[-2:]) for s in oprev_melt['oprev_label']]

        parent_rnd_method_melted = pd.merge(
            rnd_melt,
            oprev_melt,
            left_on=['bvd9', 'year'],
            right_on=['bvd9', 'year'],
            how='left')

        parent_rnd_method_melted['parent_rnd_clean'] = parent_rnd_method_melted['parent_rnd'] * \
                                                       parent_rnd_method_melted[
                                                           'parent_exposure']

        parent_rnd_method_melted['method'] = str(method)

        parent_rnd_method_melted.dropna(subset=['parent_exposure', 'parent_rnd', 'parent_rnd_clean'], how='all',
                                        inplace=True)

        parent_rnd_conso = parent_rnd_conso.append(parent_rnd_method_melted)

        report_parent_rnd.update(
            pd.DataFrame.to_dict(
                parent_rnd_method_melted[['year', 'parent_rnd_clean']].groupby(
                    ['year']).sum().rename(columns={'parent_rnd_clean': 'with_method: ' + str(method)})
            )
        )

    parent_rnd_conso_cols = ['bvd9', 'year', 'parent_oprev', 'parent_rnd', 'parent_exposure', 'parent_rnd_clean',
                             'method']

    parent_rnd_conso.to_csv(reg['parent']['rnd'],
                            columns=parent_rnd_conso_cols,
                            float_format='%.10f',
                            index=False,
                            na_rep='#N/A'
                            )

    return parent_rnd_conso
    # return report_parent_rnd, parent_rnd_conso

def compute_sub_rnd(
        sub_exposure,
        parent_rnd
):
    print('Compute subsidiary level rnd')

    sub_rnd_conso = pd.DataFrame()

    report_sub_rnd = {}

    for method in reg['methods']:
        sub_rnd = pd.DataFrame()

        sub_exposure_method = sub_exposure[sub_exposure['method'] == method]
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating subsidiary level rnd
        sub_rnd = pd.merge(
            sub_exposure_method, parent_rnd_method[['bvd9', 'parent_rnd', 'year', 'parent_rnd_clean']],
            left_on='bvd9', right_on='bvd9',
            how='left'
        )

        df = sub_rnd[
            ['bvd9', 'year', 'sub_exposure']
        ].groupby(['bvd9', 'year']).sum().rename(
            columns={'sub_exposure': 'parent_exposure_from_sub'}
        )

        sub_rnd = pd.merge(
            sub_rnd, df,
            left_on=['bvd9', 'year'], right_on=['bvd9', 'year'],
            how='left',
            suffixes=(False, False)
        )

        sub_rnd['sub_rnd_clean'] = sub_rnd['parent_rnd_clean'] * sub_rnd['sub_exposure'] / sub_rnd[
            'parent_exposure_from_sub']

        sub_rnd['method'] = str(method)

        sub_rnd_conso = sub_rnd_conso.append(sub_rnd)

        report_sub_rnd.update(
            pd.DataFrame.to_dict(
                sub_rnd[['year', 'sub_rnd_clean']].groupby(['year']).sum().rename(
                    columns={'sub_rnd_clean': 'with_method: ' + str(method)})
            )
        )

    sub_rnd_conso_cols = ['sub_bvd9', 'bvd9', 'year', 'parent_rnd_clean', 'sub_exposure', 'parent_exposure_from_sub',
                          'sub_rnd_clean', 'method']

    # melted = sub_rnd_conso.melt(
    #     id_vars=['sub_bvd9'],
    #     value_vars=reg['rnd_ys'][::-1],
    #     var_name='merge_label', value_name='sub_rnd')
    #
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg['sub']['melted'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    # Save output tables
    sub_rnd_conso.dropna(subset=['sub_rnd_clean']).to_csv(reg['sub']['rnd'],
                                                          columns=sub_rnd_conso_cols,
                                                          float_format='%.10f',
                                                          index=False,
                                                          na_rep='#N/A'
                                                          )

    return sub_rnd_conso[sub_rnd_conso_cols]
    # return report_sub_rnd, sub_rnd_conso[sub_rnd_conso_cols]

def update_report(
        report
):
    """
    Update a json file with reporting outputs and pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param reg: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    def convert(o):
        if isinstance(o, np.int32): return int(o)

    print('Update report.json file ...')

    with open(reg['case_root'].joinpath(r'report.json'), 'w') as file:
        json.dump(report, file, indent=4, default=convert)


def pprint_report(
        report
):
    """
    Pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param reg: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    def convert(o):
        if isinstance(o, np.int32): return int(o)

    with open(reg['case_root'].joinpath(r'report.txt'), 'w') as file:
        file.write('INITIALISE\n\n')

        json.dump(report['initialisation'], file, indent=4, default=convert)

        file.write('\n\n')

        file.write('NB: RnD in EUR million\n\n')

        for company_type in reg['company_types']:
            file.write('*********************************************\n')
            file.write(str(company_type.upper()) + '\n')
            file.write('*********************************************\n\n')

            file.write('SELECT PARENT COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['select_parents'], orient='index'
            ).append(
                pd.DataFrame.from_dict(
                    report['load_parent_financials'], orient='index'
                )
            )

            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('LOAD SUBSIDIARIES FROM SELECTED PARENT COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['load_subsidiary_identification'], orient='index'
            ).append(
                pd.merge(
                    pd.DataFrame.from_dict(report['select_parents_and_subsidiaries'], orient='index'),
                    pd.DataFrame.from_dict(report['keyword_screen_by_method'], orient='index'),
                    left_index=True, right_index=True
                )
            )

            # .append(
            #     pd.merge(
            #         pd.DataFrame.from_dict(report['load_subsidiary_financials'], orient='index'),
            #         pd.DataFrame.from_dict(report['screen_subsidiary_activities'], orient='index'),
            #         left_index=True, right_index=True
            #         )
            #     )

            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('COMPUTE EXPOSURE\n\n')

            file.write('at_parent_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_parent_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_subsidiary_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('COMPUTE RND\n\n')

            file.write('at_parent_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_parent_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_subsidiary_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))


def merge_sub_rnd_w_parents(
        sub_rnd,
        parent_ids,
        parent_guo_ids
):
    print('... merge with parent and guo data')

    # Get company and guo type data for subs
    parent_ids_merged = pd.merge(
        parent_ids,
        parent_guo_ids,
        left_on='guo_bvd9', right_on='guo_bvd9',
        how='left',
        suffixes=(False, False)
    )

    # Get ultimate owner company type info for subs
    sub_rnd_merged = pd.merge(
        sub_rnd,
        parent_ids_merged,
        left_on='bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    )

    return sub_rnd_merged


def merge_sub_rnd_w_countries(
        sub_rnd,
        selected_sub_ids
):
    print('... merge with country data')

    # Get country info for subs
    sub_ids_w_country = pd.merge(
        selected_sub_ids,
        country_ref[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).drop(columns=['country_2DID_iso', 'sub_country_2DID_iso'])

    sub_ids_w_country.rename(
        columns={'country_3DID_iso': 'sub_country_3DID_iso',
                 'world_player': 'sub_world_player'}, inplace=True
    )

    sub_rnd_merged = pd.merge(
        sub_rnd,
        sub_ids_w_country,
        left_on='sub_bvd9', right_on='sub_bvd9',
        how='left',
        suffixes=(False, False)
    )

    return sub_rnd_merged


def merge_sub_rnd_w_clusters(
        sub_rnd,
        selected_sub_fins
):
    print('... merge with cluster data')

    # Get keyword info for subs
    sub_rnd_merged = pd.merge(
        sub_rnd,
        selected_sub_fins[['sub_bvd9', 'keyword_mask'] + rnd_cluster_cats],
        left_on='sub_bvd9', right_on='sub_bvd9',
        how='left',
        suffixes=(False, False)
    )

    # Compute a keyword based share for each cluster and apply to subs_rnd
    sub_rnd_merged['keyword_mask'] = sub_rnd_merged[rnd_cluster_cats].sum(axis=1)

    for category in rnd_cluster_cats:
        sub_rnd_merged[category] = sub_rnd_merged['sub_rnd_clean'] * sub_rnd_merged[category] / sub_rnd_merged[
            'keyword_mask']

    sub_rnd_merged.drop(columns=['keyword_mask', 'sub_rnd_clean'], inplace=True)

    return sub_rnd_merged


def melt_n_group_sub_rnd(
        sub_rnd
):
    print('... melt and group sub_rnd')

    sub_rnd_melted = sub_rnd

    # Get keyword info for subs
    sub_rnd_melted = sub_rnd.melt(
        id_vars=['bvd9', 'sub_bvd9', 'year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type',
                 'is_listed_company', 'method'],
        value_vars=rnd_cluster_cats,
        var_name='cluster', value_name='sub_rnd_clean')

    # # TODO: Upload VCS reference table
    # # Flag parents embedded in MNC
    # mnc_ids = pd.read_csv(
    #     r'C:\Users\Simon\PycharmProjects\rnd-private\ref_tables\mnc_tracking_jrc004_to_newapp_20200420.csv',
    #     na_values='#N/A',
    #     dtype=str
    # )
    #
    # sub_rnd_melted['is_embedded_in_MNC'] = sub_rnd_melted.bvd9.isin(mnc_ids.parent_bvd9)

    # Group at parent level
    sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type', 'is_listed_company',
                            'cluster', 'method']

    # sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type', 'is_listed_company',
    #                         'method']

    # for soeur_rnd benchmark
    sub_rnd_grouped = sub_rnd_melted.groupby(sub_rnd_grouped_cols).sum()

    sub_rnd_grouped.reset_index(inplace=True)

    sub_rnd_grouped['approach'] = 'NewApp_rnd_2020_GLOBAL_20200419'

    sub_rnd_grouped['technology'] = sub_rnd_grouped['priority'] = sub_rnd_grouped['action'] = '#N/A'

    # # estimating rnd embedded in MNC
    # embedded_sub_rnd_grouped = sub_rnd_melted[sub_rnd_melted.is_embedded_in_MNC == True].groupby(
    #     sub_rnd_grouped_cols).sum()
    #
    # embedded_sub_rnd_grouped.reset_index(inplace=True)
    #
    # embedded_sub_rnd_grouped['approach'] = 'NewApp_rnd_2020_GLOBAL_20200419_in_MNC'
    #
    # embedded_sub_rnd_grouped['technology'] = embedded_sub_rnd_grouped['priority'] = embedded_sub_rnd_grouped['action'] = '#N/A'

    return sub_rnd_grouped  # , embedded_sub_rnd_grouped


def merge_n_group_sub_rnd(
        sub_rnd,
        parent_ids,
        parent_guo_ids,
        selected_sub_ids,
        selected_sub_fins
):
    sub_rnd_merged_w_parents = merge_sub_rnd_w_parents(
        sub_rnd,
        parent_ids,
        parent_guo_ids
    )

    sub_rnd_merged_w_countries = merge_sub_rnd_w_countries(
        sub_rnd_merged_w_parents,
        selected_sub_ids
    )

    sub_rnd_merged_w_clusters = merge_sub_rnd_w_clusters(
        sub_rnd_merged_w_countries,
        selected_sub_fins
    )

    sub_rnd_grouped = melt_n_group_sub_rnd(
        sub_rnd_merged_w_clusters
    )

    sub_rnd_grouped.mask(sub_rnd_grouped['is_listed_company'] == True, 'listed')
    sub_rnd_grouped.mask(sub_rnd_grouped['is_listed_company'] == False, 'unlisted guo50')

    sub_rnd_grouped.rename(columns={'is_listed_company': 'type'}, inplace=True)

    # embedded_sub_rnd_grouped.mask(embedded_sub_rnd_grouped['is_listed_company'] == True, 'listed')
    # embedded_sub_rnd_grouped.mask(embedded_sub_rnd_grouped['is_listed_company'] == False, 'unlisted guo50')
    #
    # embedded_sub_rnd_grouped.rename(columns={'is_listed_company': 'type'}, inplace=True)

    return sub_rnd_grouped  # , embedded_sub_rnd_grouped


def load_n_group_soeur_rnd():
    print('... load benchmark table')

    # soeur_rnd = pd.read_csv(
    #     'https://raw.githubusercontent.com/pysleto/mapping-tables/master/SOEUR_rnd_2019b_20200309.csv',
    #     error_bad_lines=False)

    soeur_rnd = pd.read_csv(
        r'C:\Users\Simon\PycharmProjects\rnd-private\ref_tables\SOEUR_rnd_2019b_20200309 - grouped.csv',
        na_values='#N/A'
    )

    print('... and group')

    # Group all soeur scope
    soeur_rnd_grouped = soeur_rnd.groupby(['year', 'sub_country_3DID_iso', 'sub_world_player']).sum()

    soeur_rnd_grouped.reset_index(inplace=True)

    soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309'

    soeur_rnd_grouped['method'] = '#N/A'

    soeur_rnd_grouped['type'] = soeur_rnd_grouped['cluster'] = soeur_rnd_grouped['guo_type'] = '#N/A'

    # Group soeur embedded in MNC scope
    embedded_soeur_rnd_grouped = soeur_rnd[soeur_rnd.is_embedded_in_MNC == True].groupby(
        ['year', 'sub_country_3DID_iso', 'sub_world_player']).sum()

    embedded_soeur_rnd_grouped.reset_index(inplace=True)

    embedded_soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309_in_MNC'

    embedded_soeur_rnd_grouped['method'] = '#N/A'

    embedded_soeur_rnd_grouped['type'] = embedded_soeur_rnd_grouped['cluster'] = embedded_soeur_rnd_grouped[
        'guo_type'] = '#N/A'

    return (soeur_rnd_grouped, embedded_soeur_rnd_grouped)


def load_n_group_MNC_rnd():
    print('... load benchmark table')

    mnc_rnd_grouped = pd.read_csv(
        'https://raw.githubusercontent.com/pysleto/mapping-tables/master/NewApp_MNC_rnd_2019_20190731.csv',
        error_bad_lines=False)

    mnc_rnd_grouped['approach'] = 'NewApp_MNC_rnd_2019_20190731'

    mnc_rnd_grouped['method'] = '#N/A'

    mnc_rnd_grouped['type'] = mnc_rnd_grouped['cluster'] = mnc_rnd_grouped['guo_type'] = '#N/A'

    return mnc_rnd_grouped
