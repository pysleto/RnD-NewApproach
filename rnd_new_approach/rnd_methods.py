# Import libraries
import sys

from config import registry as reg
from config import col_labels as col

import pandas as pd
import numpy as np

import json
from tabulate import tabulate

from data_input import file_loader as load

from benchmark import by_methods as by_mtd

# Import mapping tables
ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))


def load_parent_ids(stage, conso_ids):
    """
    Load identification data for parent companies
    """

    parent_ids = parent_conso = pd.DataFrame()

    report = {}

    print('Read parent company ids from input tables')

    if stage == 'collection':

        for company_type in reg.company_types:
            print('...', company_type, '-', stage)

            df = load.comp_ids_from_orbis_xls(
                'parent',
                reg.case_path.joinpath(r'input/parent_ids'),
                stage,
                company_type
            )

            parent_ids = parent_ids.append(df)

        # print('Initial count')
        #
        # print('parent_bvd9:' + str(pd.Series(parent_ids.bvd9.unique()).count()))
        # print('guo_bvd9:' + str(pd.Series(parent_ids.guo_bvd9.unique()).count()))

        # # Parents without any identified corporate GUO50 or GUO25 are considered as Highest Corporate Owner
        parent_ids['guo_bvd9'] = np.where(parent_ids['guo_bvd9'].isna(), parent_ids['bvd9'], parent_ids['guo_bvd9'])

        # print('guo_bvd9_update:' + str(pd.Series(parent_ids.guo_bvd9.unique()).count()))

        # guo_ids = pd.Series(parent_ids.guo_bvd9.unique())

        # parent_conso['bvd9'] = parent_ids['bvd9'].append(guo_conso)

        # parent_ids['is_parent'] = np.where(parent_ids['bvd9'].isin(parent_ids['bvd9']), True, False)
        # parent_ids['is_GUO'] = np.where(parent_ids['bvd9'].isin(guo_ids), True, False)

        parent_ids.dropna(subset=['guo_bvd9', 'bvd9'], inplace=True)
        parent_ids.drop_duplicates(subset=['guo_bvd9', 'bvd9'], keep='first', inplace=True)

        return parent_ids[col.comp_ids[stage]['parent']]

    elif stage == 'consolidation':

        print('... consolidated scope')

        parent_ids = load.comp_ids_from_orbis_xls(
            'parent',
            reg.case_path.joinpath(r'input/parent_ids'),
            stage,
            'all'
        )

        # print('As retrieved')
        #
        # print('parent_bvd9:' + str(pd.Series(parent_ids.bvd9.unique()).count()))
        # print('guo_bvd9:' + str(pd.Series(parent_ids.guo_bvd9.unique()).count()))

        # Parents without any identified corporate GUO50 or GUO25 are considered as Highest Corporate Owner
        parent_ids['guo_bvd9'] = np.where(parent_ids['guo_bvd9'].isna(), parent_ids['bvd9'], parent_ids['guo_bvd9'])

        # print('guo_bvd9_update:' + str(pd.Series(parent_ids.guo_bvd9.unique()).count()))

        parent_ids['is_quoted'] = np.where(parent_ids['quoted'] == 'Yes', True, False)
        # parent_ids['is_parent'] = np.where(
        #     parent_ids.bvd9.isin(conso_ids.loc[conso_ids['is_parent'], 'bvd9']), True, False
        # )
        # parent_ids['is_GUO'] = np.where(parent_ids.bvd9.isin(conso_ids.loc[conso_ids['is_GUO'], 'bvd9']), True, False)

        parent_ids.drop_duplicates(
            subset=['guo_bvd9', 'guo_country_2DID_iso', 'bvd9', 'parent_conso', 'country_2DID_iso'],
            keep='first', inplace=True)

        # Merge with ref_country for allocation to world player categories
        parent_merge = pd.merge(
            parent_ids[col.comp_ids[stage]['parent']],
            ref_country[['country_2DID_iso', 'world_player']],
            left_on='country_2DID_iso', right_on='country_2DID_iso',
            how='left',
            suffixes=(False, False)
        )

        # parent_merge = conso_merge[conso_merge['is_parent'] == True].copy()

        parent_merge.dropna(subset=['bvd9', 'parent_conso'], inplace=True)

        parent_merge.drop_duplicates(subset=['bvd9', 'parent_conso'], keep='first', inplace=True)

        # TODO: Implement specific collection for GUO ids
        # Merge with ref_country for allocation to world player categories
        guo_merge = pd.merge(
            parent_ids[col.comp_ids[stage]['guo']],
            ref_country[['country_2DID_iso', 'world_player']],
            left_on='guo_country_2DID_iso', right_on='country_2DID_iso',
            how='left',
            suffixes=(False, False)
        )

        guo_merge.dropna(subset=['guo_bvd9'], inplace=True)

        guo_merge.rename(
            columns={'world_player': 'guo_world_player'},
            inplace=True)

        guo_merge.drop_duplicates(keep='first', inplace=True)

        return parent_merge[col.comp_ids[stage]['parent'] + ['world_player']], guo_merge[
            col.comp_ids[stage]['guo'] + ['guo_world_player']]


def load_sub_ids(stage):
    """
    Consolidate a unique list of subsidiaries
    """
    # Initialize DF
    sub_ids = pd.DataFrame()
    report = {}

    print('Read subsidiary identification input tables')

    sub_ids = load.comp_ids_from_orbis_xls(
        'sub',
        reg.case_path.joinpath(r'input/sub_ids'),
        stage,
        'all'
    )

    print('from excel:' + str(stage))

    print('all_sub_bvd9:' + str(pd.Series(sub_ids.sub_bvd9.unique()).count()))

    # Drop not bvd identified subsidiaries and (group,subs) duplicates
    sub_ids.dropna(subset=['sub_bvd9'], inplace=True)
    sub_ids.drop_duplicates(keep='first', inplace=True)

    print('all_sub_bvd9:' + str(pd.Series(sub_ids.sub_bvd9.unique()).count()))

    # report['Claimed by parent companies'] = {'selected_bvd9': sub_ids['bvd9'].nunique(),
    #                                          'sub_bvd9_in_selected_bvd9': sub_ids['sub_bvd9'].count().sum(),
    #                                          'unique_sub_bvd9': sub_ids['sub_bvd9'].nunique()
    #                                          }

    if stage == 'consolidation':
        sub_ids['is_quoted'] = np.where(sub_ids['quoted'] == 'Yes', True, False)

    print('Merge with ref_country ...')

    # Merge with ref_country for allocation to world player categories
    sub_ids = pd.merge(
        sub_ids,
        ref_country[['country_2DID_iso', 'world_player']],
        left_on='sub_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=('', '_ref')
    ).rename(columns={'world_player': 'sub_world_player'})
    #
    # sub_merge.drop_duplicates(keep='first', inplace=True)

    return sub_ids[col.comp_ids[stage]['sub']]
    # return report, sub_ids


def load_comp_fins(level):
    """
    Load financials data for parent companies
    """
    comp_fins = pd.DataFrame()

    print('Read parent companies financials input files')

    comp_fins = load.comp_fins_from_orbis_xls(
        level,
        reg.case_path.joinpath(r'input', level + '_fins'),
        reg.oprev_ys,
        reg.rnd_ys
    )

    # print('bvd9:' + str(pd.Series(comp_fins.bvd9.unique()).count()))

    # comp_fins.dropna(subset=reg.rnd_ys + reg.oprev_ys, how='all', inplace=True)
    if level == 'parent':
        comp_fins.dropna(subset=['bvd9'], inplace=True)
        comp_fins.drop_duplicates(subset=['bvd9', 'parent_conso'], keep='first', inplace=True)
    elif level == 'sub':
        comp_fins.dropna(subset=['sub_bvd9'], inplace=True)
        comp_fins.drop_duplicates(subset=['sub_bvd9', 'sub_conso'], keep='first', inplace=True)

    # print('bvd9_with_fins:' + str(pd.Series(comp_fins.bvd9.unique()).count()))

    for cols in reg.rnd_ys:
        comp_fins[comp_fins[cols] < 0] = 0

    comp_fins['rnd_sum'] = comp_fins[reg.rnd_ys].sum(axis=1, skipna=True)
    comp_fins['oprev_sum'] = comp_fins[reg.oprev_ys].sum(axis=1, skipna=True)

    comp_fins.dropna(subset=reg.rnd_ys + reg.oprev_ys, how='all', inplace=True)

    # comp_fins['Emp_number_y' + reg.LY] = comp_fins['Emp_number_y' + reg.LY].astype(int)

    # melted = comp_fins.melt(
    #     id_vars=['bvd9'],
    #     value_vars=['Emp_number_y' + reg.LY, 'operating_revenue_y' + reg.LY, 'sales_y' + reg.LY] + reg.rnd_ys[::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg.parent['fin_melted'],
    #               columns=['bvd9', 'year', 'type', 'value'],
    #               float_format='%.10f',
    #               index=False,
    #               na_rep='#N/A'
    #               )

    return comp_fins[col.comp_fins[level]]


# def load_parent_fins():
#     """
#     Load financials data for parent companies
#     """
#     parent_fins = pd.DataFrame()
#
#     print('Read parent companies financials input files')
#
#     level = 'parent'
#
#     parent_fins = load.comp_fins_from_orbis_xls(
#         level,
#         reg.case_path.joinpath(r'input', level + '_fins'),
#         reg.fin_files_n[level],
#         reg.oprev_ys,
#         reg.rnd_ys
#     )
#
#     # print('bvd9:' + str(pd.Series(parent_fins.bvd9.unique()).count()))
#
#     # parent_fins.dropna(subset=reg.rnd_ys + reg.oprev_ys, how='all', inplace=True)
#     parent_fins.drop_duplicates(subset=['bvd9', 'parent_conso'], keep='first', inplace=True)
#
#     # print('bvd9_with_fins:' + str(pd.Series(parent_fins.bvd9.unique()).count()))
#
#     for cols in reg.rnd_ys:
#         parent_fins[parent_fins[cols] < 0] = 0
#
#     parent_fins['rnd_sum'] = parent_fins[reg.rnd_ys].sum(axis=1, skipna=True)
#     parent_fins['oprev_sum'] = parent_fins[reg.oprev_ys].sum(axis=1, skipna=True)
#
#     # parent_fins['Emp_number_y' + reg.LY] = parent_fins['Emp_number_y' + reg.LY].astype(int)
#
#     # melted = parent_fins.melt(
#     #     id_vars=['bvd9'],
#     #     value_vars=['Emp_number_y' + reg.LY, 'operating_revenue_y' + reg.LY, 'sales_y' + reg.LY] + reg.rnd_ys[::-1],
#     #     var_name='merge_label', value_name='value')
#     #
#     # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
#     # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
#     #
#     # melted.to_csv(reg.parent['fin_melted'],
#     #               columns=['bvd9', 'year', 'type', 'value'],
#     #               float_format='%.10f',
#     #               index=False,
#     #               na_rep='#N/A'
#     #               )
#
#     parent_fins.dropna(subset=['bvd9'], inplace=True)
#     parent_fins.drop_duplicates(keep='first', inplace=True)
#
#     return parent_fins[col.parent_fins]


def select_parent_ids_with_rnd(
        parent_fins
):
    # Identify the top companies that constitute 99% of the R&D expenses
    start = 0.0
    count = 0

    print('Select parent companies representing ' + str(reg.rnd_limit) + ' of total RnD')

    parent_fins.sort_values(by='rnd_mean', ascending=False, na_position='last')

    while start < reg.rnd_limit * parent_fins['rnd_mean'].sum():
        count += 1
        start = parent_fins.nlargest(count, ['rnd_mean'])['rnd_mean'].sum()

    selected_parent_ids = parent_fins.nlargest(count, ['rnd_mean'])

    selected_parent_ids.drop_duplicates(subset=['bvd9'], keep='first')

    selected_parent_ids.drop_duplicates(keep='first', inplace=True)

    return selected_parent_ids


def load_sub_fins():
    """
    Loads financials for subsidiaries
    """
    sub_fins = pd.DataFrame()
    report = {}

    print('Read subsidiaries financials input tables')

    sub_fins = load.sub_fins_from_orbis_xls(
        reg.case_path.joinpath(r'input/sub_fins'),
        reg.sub_fin_files_n,
        reg.oprev_ys,
        reg.rnd_ys
    )

    # sub_fins = sub_fins[sub_fins['sub_bvd9'].isin(select_subs['sub_bvd9'])]

    print('sub_bvd9_with_fins:' + str(pd.Series(sub_fins.sub_bvd9.unique()).count()))

    # sub_fins.dropna(subset=reg.rnd_ys + reg.oprev_ys, how='all', inplace=True)
    sub_fins.drop_duplicates(subset=['sub_bvd9', 'sub_conso'], keep='first', inplace=True)

    print('sub_bvd9_with_fins:' + str(pd.Series(sub_fins.sub_bvd9.unique()).count()))

    for cols in reg.rnd_ys:
        sub_fins[sub_fins[cols] < 0] = 0

    sub_fins['rnd_sum'] = sub_fins[reg.rnd_ys].sum(axis=1, skipna=True)
    sub_fins['oprev_sum'] = sub_fins[reg.oprev_ys].sum(axis=1, skipna=True)

    # sub_fins_w_fin = sub_fins.dropna(subset=reg.oprev_ys, how='all')

    # report['Returned by ORBIS'] = {'sub_bvd9_in_selected_bvd9': sub_fins['sub_bvd9'].count().sum(),
    #                                'unique_sub_bvd9': sub_fins['sub_bvd9'].nunique(),
    #                                'unique_has_fin': sub_fins_w_fin['sub_bvd9'].nunique(),
    #                                }

    # # Merging subsidiary ref_country for allocation to world player categories and countries
    # merged = pd.merge(
    #     sub_fins_w_fin, ref_country[['country_2DID_iso', 'country_3DID_iso', 'region', 'world_player']],
    #     left_on='country_iso', right_on='country_2DID_iso',
    #     how='left',
    #     suffixes=(False, False)
    # )

    # melted = sub_fins.melt(
    #     id_vars=['sub_company_name', 'sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'],
    #     value_vars=reg.oprev_ys[::-1] + reg.rnd_ys[::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg.sub['fin_melted'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    sub_fins.drop_duplicates(keep='first', inplace=True)

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

    # for method in reg.methods:
    #     print('Flag strategy: ' + str(method))
    #
    #     report['From ORBIS with applied method: ' + str(method)] = {
    #         'selected_bvd9': sub_ids['bvd9'][sub_ids[method] == True].nunique(),
    #         'unique_sub_bvd9': sub_ids['sub_bvd9'][sub_ids[method] == True].nunique()
    #         # 'unique_has_fin': sub_ids['sub_bvd9'][
    #         #     (sub_ids[method] == True) & (sub_ids['has_fin'] == True)].nunique()
    #     }

    sub_ids.drop_duplicates(keep='first', inplace=True)

    return sub_ids
    # return report, sub_ids


def screen_sub_fins_for_keywords(
        sub_fins
):
    print('Screen subsidiary activity for keywords')

    report = {}

    for category in reg.categories:

        sub_fins[category] = False

        for keyword in reg.keywords[category]:
            sub_fins[category] |= sub_fins['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['products_services_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['full_overview_desc'].str.contains(keyword, case=False, regex=False)

    # screen_subs = sub_fins.loc[:, ['sub_company_name', 'sub_bvd9', 'sub_bvd_id'] + reg.categories]

    sub_fins['sub_turnover_sum'] = sub_fins.loc[:, reg.oprev_ys_for_exp].sum(axis=1)

    sub_fins['keyword_mask'] = list(
        map(bool, sub_fins[[cat for cat in reg.categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    sub_fins['sub_turnover_sum_masked'] = sub_fins['sub_turnover_sum'].mask(~sub_fins['keyword_mask'])

    # report['Returned by ORBIS'] = {
    #     'unique_is_matching_a_keyword': sub_fins['sub_bvd9'][sub_fins['keyword_mask'] == True].nunique()
    # }

    sub_fins.drop_duplicates(keep='first', inplace=True)

    return sub_fins
    # return report, sub_fins


def compute_exposure(
        selected_sub_ids,
        sub_fins
):
    sub_exposure_conso = pd.DataFrame()
    parent_exposure_conso = pd.DataFrame()
    # report_keyword_match = {}
    # report_exposure = {'at_subsidiary_level': {}, 'at_parent_level': {}}

    print('Compute exposure for strategy:')

    for method in reg.methods:
        print('... ' + str(method))
        sub_exposure = pd.DataFrame()

        # Merging selected subsidiaries by method with masked turnover and turnover
        sub_exposure = pd.merge(
            selected_sub_ids[selected_sub_ids[method] == True],
            sub_fins,
            left_on=['sub_unique_id'], right_on=['sub_unique_id'],
            how='left',
            suffixes=('', '_fins')
        )

        print('Merge with ref_country ...')

        # Merge with ref_country for allocation to world player categories
        sub_exposure = pd.merge(
            sub_exposure,
            ref_country[['country_2DID_iso', 'world_player']],
            left_on='country_2DID_iso', right_on='country_2DID_iso',
            how='left',
            suffixes=('', '_ref')
        )

        sub_exposure.rename(columns={'company_name': 'parent_name'}, inplace=True)

        sub_exposure['keyword_mask'] = np.where(sub_exposure['keyword_mask'] == True, 1, 0)

        # Calculating group exposure
        parent_exposure = sub_exposure[
            ['parent_unique_id', 'bvd9', 'parent_name', 'parent_conso', 'country_2DID_iso', 'world_player',
             'keyword_mask', 'sub_turnover_sum_masked', 'sub_turnover_sum']
        ].groupby(['parent_unique_id', 'bvd9', 'parent_name', 'parent_conso', 'country_2DID_iso', 'world_player']).sum()

        parent_exposure.reset_index(inplace=True)

        parent_exposure.rename(
            columns={'country_2DID_iso': 'parent_country_2DID_iso',
                     'world_player': 'parent_world_player',
                     'keyword_mask': 'keyword_mask_sum_in_parent',
                     'sub_turnover_sum': 'total_sub_turnover_sum_in_parent',
                     'sub_turnover_sum_masked': 'total_sub_turnover_sum_masked_in_parent'},
            inplace=True
        )

        parent_exposure['parent_exposure'] = parent_exposure['total_sub_turnover_sum_masked_in_parent'] / \
                                             parent_exposure['total_sub_turnover_sum_in_parent']

        parent_exposure['method'] = str(method)

        parent_exposure_conso = parent_exposure_conso.append(parent_exposure)

        # parent_exposure_conso.dropna(subset=['parent_exposure'], inplace=True)

        # Calculating subsidiary level exposure
        sub_exposure = pd.merge(
            sub_exposure, parent_exposure[
                ['parent_unique_id', 'bvd9', 'parent_country_2DID_iso', 'parent_world_player', 'keyword_mask_sum_in_parent',
                 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent', 'parent_exposure', 'method']],
            left_on='parent_unique_id', right_on='parent_unique_id',
            how='left'
        )

        sub_exposure['sub_exposure'] = sub_exposure['sub_turnover_sum_masked'] / sub_exposure[
            'total_sub_turnover_sum_in_parent']

        # report_keyword_match['From ORBIS with applied method: ' + str(method)] = {
        #     'sub_bvd9_in_selected_bvd9': selected_sub_ids['sub_bvd9'][selected_sub_ids[method] == True].count().sum(),
        #     'unique_is_matching_a_keyword': sub_exposure['sub_bvd9'][sub_exposure['keyword_mask'] == True].nunique()
        # }
        #
        # report_exposure['at_parent_level'].update({
        #     'With method: ' + str(method): {
        #         'Total_exposure': parent_exposure['parent_exposure'].sum()
        #     }
        # })
        #
        # report_exposure['at_subsidiary_level'].update({
        #     'With method: ' + str(method): {
        #         'Total_exposure': sub_exposure['sub_exposure'].sum()
        #     }
        # })

        sub_exposure_conso = sub_exposure_conso.append(sub_exposure)

    # sub_exposure_conso.dropna(subset=['sub_exposure'], inplace=True)

    parent_exposure_conso.drop_duplicates(subset=['parent_unique_id'], keep='first',
                                          inplace=True)
    sub_exposure_conso.drop_duplicates(
        subset=['parent_unique_id', 'sub_unique_id'],
        keep='first', inplace=True)

    return parent_exposure_conso, sub_exposure_conso
    # return report_keyword_match, report_exposure, parent_exposure_conso, sub_exposure_conso


def compute_parent_rnd(
        parent_exposure,
        parent_fins
):
    print('Compute parent level rnd')

    parent_rnd_conso = pd.DataFrame()

    report_parent_rnd = {}

    parent_rnd = pd.merge(
        parent_exposure[['parent_unique_id', 'bvd9', 'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player',
                         'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
                         'parent_exposure', 'method']],
        parent_fins,
        left_on='parent_unique_id', right_on='parent_unique_id',
        how='left',
        suffixes=('', '_fins')
    )

    for method in reg.methods:
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating group level rnd
        rnd_melt = parent_rnd_method.melt(
            id_vars=['parent_unique_id', 'bvd9', 'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player',
                     'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent', 'parent_exposure'],
            value_vars=reg.rnd_ys,
            var_name='rnd_label', value_name='parent_rnd')

        rnd_melt['year'] = [int('20' + s[-2:]) for s in rnd_melt['rnd_label']]

        oprev_melt = parent_rnd_method.melt(
            id_vars=['bvd9'],
            value_vars=reg.oprev_ys,
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

        # parent_rnd_method_melted.dropna(subset=['parent_exposure', 'parent_rnd', 'parent_rnd_clean'], how='all',
        #                                 inplace=True)

        parent_rnd_conso = parent_rnd_conso.append(parent_rnd_method_melted)

        # report_parent_rnd.update(
        #     pd.DataFrame.to_dict(
        #         parent_rnd_method_melted[['year', 'parent_rnd_clean']].groupby(
        #             ['year']).sum().rename(columns={'parent_rnd_clean': 'with_method: ' + str(method)})
        #     )
        # )

    parent_rnd_conso.drop_duplicates(subset=['parent_unique_id', 'year'], keep='first',
                                     inplace=True)

    return parent_rnd_conso
    # return report_parent_rnd, parent_rnd_conso


def compute_guo_rnd(
        parent_rnd,
        parent_ids,
        guo_ids
):
    print('Compute guo level rnd')

    guo_rnd_conso = pd.DataFrame()

    # Merging with guo_ids

    # print('parent_rnd_clean = ' + str(parent_rnd.parent_rnd_clean.sum()))
    # print('parent_rnd = ' + str(parent_rnd.parent_rnd.sum()))
    # print('parent_oprev = ' + str(parent_rnd.parent_oprev.sum()))
    # print('parent_exposure = ' + str(parent_rnd.parent_exposure.sum()))

    parent_ids = parent_ids[['bvd9', 'company_name', 'guo_bvd9']].drop_duplicates(subset=['bvd9', 'guo_bvd9'])

    parent_rnd = pd.merge(
        parent_rnd,
        parent_ids,
        left_on='bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    )

    for method in reg.methods:
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # guo_rnd_method_ungrouped = guo_rnd_method[guo_rnd_method['guo_bvd9'].isna()].copy()
        #
        # guo_rnd_method_ungrouped['guo_bvd9'] = guo_rnd_method_ungrouped['bvd9']
        #
        # guo_rnd_method_ungrouped['guo_name'] = guo_rnd_method_ungrouped['company_name']
        #
        # guo_rnd_method_ungrouped['is_guo'] = False
        #
        # guo_rnd_method_ungrouped.rename(columns={
        #     'parent_oprev': 'guo_oprev',
        #     'parent_rnd': 'guo_rnd',
        #     'parent_exposure': 'guo_exposure',
        #     'parent_rnd_clean': 'guo_rnd_clean'
        # }, inplace=True)

        guo_rnd_method = parent_rnd_method.groupby(['guo_bvd9', 'year']).agg(
            {'parent_oprev': 'sum', 'parent_rnd': 'sum', 'parent_rnd_clean': 'sum', 'parent_exposure': 'mean'}
        )

        guo_rnd_method.reset_index(inplace=True)

        guo_rnd_method.rename(columns={
            'parent_oprev': 'guo_oprev_from_parent',
            'parent_rnd': 'guo_rnd_from_parent',
            'parent_exposure': 'guo_exposure_from_parent',
            'parent_rnd_clean': 'guo_rnd_clean_from_parent'
        }, inplace=True)

        guo_rnd_method['guo_exposure'] = guo_rnd_method['guo_rnd_clean_from_parent'] / guo_rnd_method[
            'guo_rnd_from_parent']

        guo_rnd_method['method'] = str(method)

        guo_rnd_conso = guo_rnd_conso.append(guo_rnd_method)

    guo_rnd_conso = pd.merge(
        guo_rnd_conso,
        guo_ids,
        left_on='guo_bvd9', right_on='guo_bvd9',
        how='left',
        suffixes=(False, False)
    )

    # print('parent_rnd_clean = ' + str(guo_rnd_conso.guo_rnd_clean.sum()))
    # print('parent_rnd = ' + str(guo_rnd_conso.guo_rnd.sum()))
    # print('parent_oprev = ' + str(guo_rnd_conso.guo_oprev.sum()))
    # print('parent_exposure = ' + str(guo_rnd_conso.guo_exposure.sum()))

    guo_rnd_conso.drop_duplicates(keep='first', inplace=True)

    return guo_rnd_conso
    # return report_parent_rnd, parent_rnd_conso


def compute_sub_rnd(
        sub_exposure,
        parent_rnd
):
    print('Compute subsidiary level rnd')

    sub_rnd_conso = pd.DataFrame()

    report_sub_rnd = {}

    for method in reg.methods:
        sub_rnd = pd.DataFrame()

        sub_exposure_method = sub_exposure[sub_exposure['method'] == method]
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating subsidiary level rnd
        sub_rnd = pd.merge(
            sub_exposure_method,
            parent_rnd_method[['parent_unique_id', 'bvd9', 'parent_rnd', 'year', 'parent_rnd_clean']],
            left_on='parent_unique_id', right_on='parent_unique_id',
            how='left',
            suffixes=(False, False)
        )

        # df = sub_rnd[
        #     ['bvd9', 'year', 'sub_exposure']
        # ].groupby(['bvd9', 'year']).sum().rename(
        #     columns={'sub_exposure': 'parent_exposure_from_sub'}
        # )
        #
        # sub_rnd = pd.merge(
        #     sub_rnd, df,
        #     left_on=['bvd9', 'year'], right_on=['bvd9', 'year'],
        #     how='left',
        #     suffixes=(False, False)
        # )

        # sub_rnd['sub_rnd_clean'] = sub_rnd['parent_rnd_clean'] * sub_rnd['sub_exposure'] / sub_rnd[
        #     'parent_exposure_from_sub']

        sub_rnd['sub_rnd_clean'] = sub_rnd['parent_rnd_clean'] * sub_rnd['sub_exposure'] / sub_rnd[
            'parent_exposure']

        sub_rnd['method'] = str(method)

        sub_rnd_conso = sub_rnd_conso.append(sub_rnd)

        # report_sub_rnd.update(
        #     pd.DataFrame.to_dict(
        #         sub_rnd[['year', 'sub_rnd_clean']].groupby(['year']).sum().rename(
        #             columns={'sub_rnd_clean': 'with_method: ' + str(method)})
        #     )
        # )



    # melted = sub_rnd_conso.melt(
    #     id_vars=['sub_bvd9'],
    #     value_vars=reg.rnd_ys[::-1],
    #     var_name='merge_label', value_name='sub_rnd')
    #
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(reg.sub['melted'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    sub_rnd_conso.dropna(subset=['sub_rnd_clean'], inplace=True)

    sub_rnd_conso.drop_duplicates(
        subset=['parent_unique_id', 'sub_unique_id',
                'year'],
        keep='first', inplace=True)

    return sub_rnd_conso
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

    with open(reg.case_path.joinpath(r'report.json'), 'w') as file:
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

    with open(reg.case_path.joinpath(r'report.txt'), 'w') as file:
        file.write('INITIALISE\n\n')

        json.dump(report['initialisation'], file, indent=4, default=convert)

        file.write('\n\n')

        file.write('NB: RnD in EUR million\n\n')

        for company_type in reg.company_types:
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


def group_soeur_rnd_for_bench(ref_soeur_path):
    print('... load benchmark table')

    soeur_rnd = pd.read_csv(ref_soeur_path, na_values='#N/A')

    print('... and group')

    soeur_rnd_grouped = soeur_rnd.groupby(['year', 'sub_country_2DID_iso', 'sub_world_player']).sum()

    soeur_rnd_grouped.reset_index(inplace=True)

    soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309'

    soeur_rnd_grouped['method'] = 'keep_all'

    soeur_rnd_grouped['vintage'] = soeur_rnd_grouped['approach'] + ' - ' + soeur_rnd_grouped['method']

    soeur_rnd_grouped['type'] = soeur_rnd_grouped['cluster'] = soeur_rnd_grouped['guo_type'] = '#N/A'

    soeur_rnd_grouped.rename(columns={
        'sub_country_3DID_iso': 'country_3DID_iso',
        'sub_world_player': 'world_player',
        'sub_rnd_clean': 'rnd_clean'
    }, inplace=True)

    return soeur_rnd_grouped


def group_soeur_rnd_for_bench(ref_soeur_path):
    print('... load benchmark table')

    soeur_rnd = pd.read_csv(ref_soeur_path, na_values='#N/A')

    print('... and group')

    soeur_rnd_grouped = soeur_rnd.groupby(['year', 'sub_country_2DID_iso', 'sub_world_player']).sum()

    soeur_rnd_grouped.reset_index(inplace=True)

    soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309'

    soeur_rnd_grouped['method'] = 'keep_all'

    soeur_rnd_grouped['vintage'] = soeur_rnd_grouped['approach'] + ' - ' + soeur_rnd_grouped['method']

    soeur_rnd_grouped['type'] = soeur_rnd_grouped['cluster'] = soeur_rnd_grouped['guo_type'] = '#N/A'

    soeur_rnd_grouped.rename(columns={
        'sub_country_3DID_iso': 'country_3DID_iso',
        'sub_world_player': 'world_player',
        'sub_rnd_clean': 'rnd_clean'
    }, inplace=True)

    return soeur_rnd_grouped
