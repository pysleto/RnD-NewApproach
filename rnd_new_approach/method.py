import pandas as pd
from data_input import input


def load_parent_ids(cases, files, country_map):
    """
    Load identification data for parent companies
    """

    parent_ids = df_cache = pd.DataFrame()

    report = {}

    print('Read parent company ids from input tables')

    for company_type in cases['COMPANY_TYPES']:
        print('... ' + str(company_type))

        df = input.import_parent_ids_from_orbis_xls(
            cases['CASE_ROOT'].joinpath(r'input/parent_ids'),
            cases['PARENT_ID_FILES_N'][company_type],
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
    for company_type in cases['COMPANY_TYPES']:
        parent_ids['is_' + str(company_type)] = False
        parent_ids.loc[parent_ids['bvd9'].isin(df_cache[company_type]), 'is_' + str(company_type)] = True

    # Define column ids
    id_columns = ['bvd9', 'company_name', 'bvd_id', 'legal_entity_id', 'guo_bvd9'] + \
                 ['is_' + str(company_type) for company_type in cases['COMPANY_TYPES']] + \
                 ['NACE_4Dcode', 'NACE_desc', 'subs_n'] + \
                 ['country_2DID_iso']

    print('Merge with country_map ...')

    # Merge with country_map for allocation to world player categories
    id_merge = pd.merge(
        parent_ids[id_columns],
        country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    guo_columns = ['guo_bvd9', 'guo_type', 'guo_name', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso']

    guo_merge = pd.merge(
        parent_ids[guo_columns],
        country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='guo_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).rename(columns={'country_3DID_iso': 'guo_country_3DID_iso', 'world_player': 'guo_world_player'})

    guo_merge.drop_duplicates(['guo_bvd9'], keep='first', inplace=True)

    print('Save parent company ids output files ...')

    # Save output table of selected parent companies
    id_merge.to_csv(files['OUTPUT']['PARENTS']['ID'],
                    columns=id_columns + ['country_3DID_iso', 'world_player'],
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    guo_merge.to_csv(files['OUTPUT']['PARENTS']['GUO'],
                     columns=guo_columns + ['guo_country_3DID_iso', 'guo_world_player'],
                     float_format='%.10f',
                     index=False,
                     na_rep='#N/A'
                     )

    return report, id_merge, guo_merge


def load_parent_fins(cases, files, range_ys):
    """
    Load financials data for parent companies
    """
    parent_fins = pd.DataFrame()
    report = {}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    print('Read parent companies financials input files')

    parent_fins = input.import_parent_fins_from_orbis_xls(
        cases['CASE_ROOT'].joinpath(r'input/parent_fins'),
        cases['PARENT_FIN_FILES_N'],
        oprev_ys,
        rnd_ys,
        LY
    )

    parent_fins = parent_fins.dropna(subset=rnd_ys, how='all')

    for cols in rnd_ys:
        parent_fins[parent_fins[cols] < 0] = 0

    parent_fins['rnd_mean'] = parent_fins[rnd_ys].mean(axis=1, skipna=True)

    parent_fin_cols = ['bvd9', 'Emp_number_y' + LY, 'sales_y' + LY,
                       'rnd_mean'] + rnd_ys[::-1] + oprev_ys[::-1]

    # parent_fins['Emp_number_y' + LY] = parent_fins['Emp_number_y' + LY].astype(int)

    # Save it as csv
    parent_fins.to_csv(files['OUTPUT']['PARENTS']['FIN'],
                       columns=parent_fin_cols,
                       float_format='%.10f',
                       index=False,
                       na_rep='#N/A'
                       )

    # melted = parent_fins.melt(
    #     id_vars=['bvd9'],
    #     value_vars=['Emp_number_y' + LY, 'operating_revenue_y' + LY, 'sales_y' + LY] + rnd_ys[::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(files['OUTPUT']['PARENTS']['FIN_MELTED'],
    #               columns=['bvd9', 'year', 'type', 'value'],
    #               float_format='%.10f',
    #               index=False,
    #               na_rep='#N/A'
    #               )

    return report, parent_fins


def select_parent_ids_with_rnd(parent_fins, rnd_limit):
    # Identify the top companies that constitute 99% of the R&D expenses
    start = 0.0
    count = 0

    print('Select parent companies representing ' + str(rnd_limit) + ' of total RnD')

    parent_fins.sort_values(by='rnd_mean', ascending=False, na_position='last')

    while start < rnd_limit * parent_fins['rnd_mean'].sum():
        count += 1
        start = parent_fins.nlargest(count, ['rnd_mean'])['rnd_mean'].sum()

    selected_parent_ids = parent_fins.nlargest(count, ['rnd_mean'])

    selected_parent_ids.drop_duplicates(subset=['bvd9'], keep='first')

    return selected_parent_ids


def load_sub_ids(cases, files, country_map):
    """
    Consolidate a unique list of subsidiaries
    """
    # Initialize DF
    sub_ids = pd.DataFrame()
    report = {}

    print('Read subsidiary identification input tables')

    sub_ids = input.import_sub_ids_from_orbis_xls(
        cases['CASE_ROOT'].joinpath(r'input/sub_ids'),
        cases['SUB_ID_FILES_N']
    )

    # Drop not bvd identified subsidiaries and (group,subs) duplicates
    sub_ids = sub_ids.dropna(subset=['bvd9', 'sub_bvd9']).drop_duplicates(['bvd9', 'sub_bvd9'], keep='first')

    report['Claimed by parent companies'] = {'selected_bvd9': sub_ids['bvd9'].nunique(),
                                             'sub_bvd9_in_selected_bvd9': sub_ids['sub_bvd9'].count().sum(),
                                             'unique_sub_bvd9': sub_ids['sub_bvd9'].nunique()
                                             }

    print('Merge with country_map ...')

    # Merge with country_map for allocation to world player categories
    id_merge = pd.merge(
        sub_ids[['company_name', 'bvd9', 'sub_company_name', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id',
                 'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl']],
        country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).rename(columns={'country_3DID_iso': 'sub_country_3DID_iso', 'world_player': 'sub_world_player'})

    print('Save subsidiaries output file ...')

    sub_id_cols = ['sub_bvd9', 'bvd9', 'sub_company_name', 'sub_bvd_id',
                   'sub_legal_entity_id', 'sub_lvl', 'sub_country_2DID_iso', 'sub_country_3DID_iso', 'sub_world_player']

    # Save it as csv
    id_merge.to_csv(files['OUTPUT']['SUBS']['ID'],
                    columns=sub_id_cols,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    return report, sub_ids


def load_sub_fins(cases, files, range_ys):
    """
    Loads financials for subsidiaries
    """
    sub_fins = pd.DataFrame()
    report = {}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    print('Read subsidiaries financials input tables')

    sub_fins = input.import_sub_fins_from_orbis_xls(
        cases['CASE_ROOT'].joinpath(r'input/sub_fins'),
        cases['SUB_FIN_FILES_N'],
        oprev_ys,
        rnd_ys,
        LY
    )

    # sub_fins = sub_fins[sub_fins['sub_bvd9'].isin(select_subs['sub_bvd9'])]

    sub_fins = sub_fins.drop_duplicates('sub_bvd9')

    for cols in rnd_ys:
        sub_fins[sub_fins[cols] < 0] = 0

    sub_fins_w_fin = sub_fins.dropna(subset=oprev_ys, how='all')

    report['Returned by ORBIS'] = {'sub_bvd9_in_selected_bvd9': sub_fins['sub_bvd9'].count().sum(),
                                   'unique_sub_bvd9': sub_fins['sub_bvd9'].nunique(),
                                   'unique_has_fin': sub_fins_w_fin['sub_bvd9'].nunique(),
                                   }

    # # Merging subsidiary country_map for allocation to world player categories and countries
    # merged = pd.merge(
    #     sub_fins_w_fin, country_map[['country_2DID_iso', 'country_3DID_iso', 'region', 'world_player']],
    #     left_on='country_iso', right_on='country_2DID_iso',
    #     how='left',
    #     suffixes=(False, False)
    # )

    sub_fins_cols = ['sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'] + \
                    oprev_ys[::-1] + rnd_ys[::-1]

    # Save it as csv
    sub_fins.to_csv(files['OUTPUT']['SUBS']['FIN'],
                    columns=sub_fins_cols,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    # melted = sub_fins.melt(
    #     id_vars=['sub_company_name', 'sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'],
    #     value_vars=oprev_ys[::-1] + rnd_ys[::-1],
    #     var_name='merge_label', value_name='value')
    #
    # melted['type'] = [str(s[:-4]) for s in melted['merge_label']]
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(files['OUTPUT']['SUBS']['FIN_MELTED'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    return report, sub_fins


def screen_sub_ids_for_method(cases, files, sub_ids):
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
    sub_ids.loc[sub_ids['sub_bvd9'].duplicated(keep=False), 'is_sub_a_duplicate'] = True
    sub_ids.loc[sub_ids['is_sub_a_duplicate'] != True, 'is_sub_a_duplicate'] = False

    sub_ids.loc[:, 'keep_all'] = True

    sub_ids.loc[~sub_ids['bvd9'].isin(sub_ids['sub_bvd9']), 'keep_comps'] = True
    sub_ids.loc[sub_ids['keep_comps'] != True, 'keep_comps'] = False
    sub_ids.loc[~sub_ids['sub_bvd9'].isin(sub_ids['bvd9']), 'keep_subs'] = True
    sub_ids.loc[sub_ids['keep_subs'] != True, 'keep_subs'] = False

    for method in cases['METHODS']:
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
    sub_ids.to_csv(files['OUTPUT']['SUBS']['ID'],
                   columns=sub_ids_cols,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

    return report, sub_ids


def screen_sub_fins_for_keywords(cases, files, range_ys, keywords, sub_fins):
    print('Screen subsidiary activity for keywords')

    categories = list(keywords.keys())

    report = {}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    for category in categories:

        sub_fins[category] = False

        for keyword in keywords[category]:
            sub_fins[category] |= sub_fins['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['products&services_desc'].str.contains(keyword, case=False, regex=False) | \
                                  sub_fins['full_overview_desc'].str.contains(keyword, case=False, regex=False)

    # screen_subs = sub_fins.loc[:, ['sub_company_name', 'sub_bvd9', 'sub_bvd_id'] + categories]

    sub_fins['sub_turnover_sum'] = sub_fins.loc[:, oprev_ys].sum(axis=1)

    sub_fins['keyword_mask'] = list(
        map(bool, sub_fins[[cat for cat in categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    sub_fins['sub_turnover_sum_masked'] = sub_fins['sub_turnover_sum'].mask(~sub_fins['keyword_mask'])

    report['Returned by ORBIS'] = {
        'unique_is_matching_a_keyword': sub_fins['sub_bvd9'][sub_fins['keyword_mask'] == True].nunique()
    }

    sub_fins_cols = ['sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc'] + \
                    oprev_ys[::-1]

    # Save it as csv
    sub_fins.to_csv(files['OUTPUT']['SUBS']['FIN'],
                    columns=sub_fins_cols +
                            ['sub_turnover_sum', 'sub_turnover_sum_masked', 'keyword_mask'] +
                            [cat for cat in categories],
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

    return report, sub_fins


def compute_exposure(cases, files, range_ys, selected_sub_ids, sub_fins):
    sub_exposure_conso = pd.DataFrame()
    parent_exposure_conso = pd.DataFrame()
    report_keyword_match = {}
    report_exposure = {'at_subsidiary_level': {}, 'at_parent_level': {}}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    print('Compute exposure for strategy:')

    for method in cases['METHODS']:
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
    parent_exposure_conso.to_csv(files['OUTPUT']['PARENTS']['EXPO'],
                                 columns=parent_exposure_cols,
                                 float_format='%.10f',
                                 index=False,
                                 na_rep='#N/A'
                                 )

    sub_exposure_conso.to_csv(files['OUTPUT']['SUBS']['EXPO'],
                              columns=['sub_bvd9',
                                       'sub_turnover_sum',
                                       'sub_turnover_sum_masked',
                                       'sub_exposure'] + parent_exposure_cols,
                              float_format='%.10f',
                              index=False,
                              na_rep='#N/A'
                              )

    return report_keyword_match, report_exposure, parent_exposure_conso, sub_exposure_conso


def compute_parent_rnd(cases, files, range_ys, parent_exposure, parent_fins):
    print('Compute parent level rnd')

    parent_rnd_conso = pd.DataFrame()

    report_parent_rnd = {}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    parent_rnd = pd.merge(parent_exposure, parent_fins,
                          left_on='bvd9', right_on='bvd9',
                          how='left'
                          )

    for method in cases['METHODS']:
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating group level rnd
        rnd_melt = parent_rnd_method.melt(
            id_vars=['bvd9', 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
                     'parent_exposure'],
            value_vars=rnd_ys,
            var_name='rnd_label', value_name='parent_rnd')

        rnd_melt['year'] = [int('20' + s[-2:]) for s in rnd_melt['rnd_label']]

        oprev_melt = parent_rnd_method.melt(
            id_vars=['bvd9'],
            value_vars=oprev_ys,
            var_name='oprev_label', value_name='parent_oprev')

        oprev_melt['year'] = [int('20' + s[-2:]) for s in oprev_melt['oprev_label']]

        parent_rnd_method_melted = pd.merge(
            rnd_melt,
            oprev_melt,
            left_on=['bvd9', 'year'],
            right_on=['bvd9', 'year'],
            how='left')

        parent_rnd_method_melted['parent_rnd_clean'] = parent_rnd_method_melted['parent_rnd'] * parent_rnd_method_melted[
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

    parent_rnd_conso.to_csv(files['OUTPUT']['PARENTS']['RND'],
                            columns=parent_rnd_conso_cols,
                            float_format='%.10f',
                            index=False,
                            na_rep='#N/A'
                            )

    return report_parent_rnd, parent_rnd_conso


def compute_sub_rnd(cases, files, range_ys, sub_exposure, parent_rnd):
    print('Compute subsidiary level rnd')

    sub_rnd_conso = pd.DataFrame()

    report_sub_rnd = {}

    oprev_ys = range_ys['oprev_ys']
    rnd_ys = range_ys['rnd_ys']
    LY = range_ys['LY']

    for method in cases['METHODS']:
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
    #     value_vars=rnd_ys[::-1],
    #     var_name='merge_label', value_name='sub_rnd')
    #
    # melted['year'] = [int('20' + s[-2:]) for s in melted['merge_label']]
    #
    # melted.to_csv(files['OUTPUT']['SUBS']['MELTED'],
    #               columns=['sub_company_name', 'sub_bvd9', 'year', 'type', 'value'],
    # float_format = '%.10f',
    # index = False,
    # na_rep = '#N/A'
    # )

    # Save output tables
    sub_rnd_conso.dropna(subset=['sub_rnd_clean']).to_csv(files['OUTPUT']['SUBS']['RND'],
                                                          columns=sub_rnd_conso_cols,
                                                          float_format='%.10f',
                                                          index=False,
                                                          na_rep='#N/A'
                                                          )

    return report_sub_rnd, sub_rnd_conso[sub_rnd_conso_cols]
