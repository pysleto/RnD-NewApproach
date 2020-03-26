from pathlib import Path
import configparser
import pandas as pd
import json
import os


def create_country_map(cases, files):
    """
    Create a country mapping table from reference file
    :param cases:
    :param cases:
    :param files:
    :param files:
    :return: Nothing
    """
    print('Read country mapping table ...')

    # Read Country mapping file
    country_map = pd.read_excel(files['MAPPING']['COUNTRY_SOURCE_PATH'],
                                sheet_name='country_map',
                                names=['country_name_iso', 'country_name_simple', 'country_2DID_iso',
                                       'country_2DID_soeur', 'country_3DID_iso', 'country_flag',
                                       'is_OECD', 'is_IEA', 'is_MI', 'region', 'IEA_region', 'world_player'
                                       ],
                                na_values='n.a.',
                                dtype={
                                    **{col: str for col in
                                       ['country_name_iso', 'country_name_simple', 'country_2DID_iso',
                                        'country_2DID_soeur', 'country_3DID_iso', 'country_flag', 'Region',
                                        'IEA_region',
                                        'world_player'
                                        ]},
                                    **{col: bool for col in ['is_OECD', 'is_IEA', 'is_MI']}
                                }
                                ).drop(columns='region')

    print('Save country mapping table ...')

    # Save it as csv
    country_map.to_csv(files['MAPPING']['COUNTRY_REFERENCE_PATH'],
                       index=False,
                       columns=['country_name_iso', 'country_name_simple', 'country_2DID_iso', 'country_2DID_soeur',
                                'country_3DID_iso', 'country_flag', 'is_OECD', 'is_IEA', 'is_MI', 'IEA_region',
                                'world_player'
                                ],
                       float_format='%.10f',
                       na_rep='n.a.'
                       )


def select_parent(cases, files, country_map, company_type):
    """
    Select the parent companies by region (having invested the most in R&D since 2010 and corresponding together to more
    than 99% of R&D investments in the region) and consolidate a global list of unique companies
    :param cases:
    :param cases:
    :param files:
    :param files:
    :param country_map: dataframe for country mapping
    :return: analytical report
    """

    parent_id = pd.DataFrame()
    report = {}

    print('Read parent companies input tables')

    # Read ORBIS input list for groups financials
    for number in list(range(1, cases['PARENTS_ID_FILE_N'][company_type] + 1)):
        print('File #' + str(number) + '/' + str(cases['PARENTS_ID_FILE_N'][company_type]))
        # Read input list of companies by world region
        df = pd.read_excel(cases['CASE_ROOT'].joinpath(
            r'input/parent_ids/' + str(company_type) + ' - identification #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank', 'company_name', 'bvd9', 'bvd_id', 'country_2DID_iso'] + ['rnd_y' + str(YY) for
                                                                                    YY in
                                                                                    range(10, 19)[::-1]],
            na_values='n.a.',
            dtype={
                **{col: str for col in ['company_name', 'bvd9', 'bvd_id', 'country_2DID_iso']},
                **{col: float for col in ['rnd_y' + str(YY) for YY in range(10, 19)]}
            }
        ).drop(columns='rank')

        df['y_lastav'] = cases['YEAR_LASTAV']

        df['rnd_mean'] = df[['rnd_y' + str(YY) for YY in range(10, 19)]].mean(axis=1, skipna=True)

        df['rnd_y_lastav'] = df['rnd_y' + str(cases['YEAR_LASTAV'])[-2:]]

        # Consolidate subsidiaries financials
        parent_id = parent_id.append(df)

    # Drop duplicates
    parent_id = parent_id.drop_duplicates(subset='bvd9', keep='first')

    print('Merging with country_map ...')

    # Merging group country_map for allocation to world player categories
    merged = pd.merge(
        parent_id, country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    print('Saving parent companies output file ...')

    # Save output table of selected parent companies
    merged.to_csv(files['OUTPUT'][company_type]['ID_EXT']['PARENTS'],
                  index=False,
                  columns=['bvd9', 'bvd_id', 'company_name', 'country_3DID_iso', 'world_player',
                           'rnd_mean', 'y_lastav', 'rnd_y_lastav'],
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return report


def load_parent_fin(cases, files, select_comp, country_map, company_type):
    """
    Loads financials for parent companies
    :param cases:
    :param cases:
    :param files:
    :param files:
    :param select_comp:
    :param select_comp:
    :return: Analytical report
    """
    parent_fin = pd.DataFrame()
    report = {}

    print('Read parent companies financials input tables')

    # Read ORBIS input list for groups financials
    for number in list(range(1, cases['PARENTS_FIN_FILE_N'][company_type] + 1)):
        print('File #' + str(number) + '/' + str(cases['PARENTS_FIN_FILE_N'][company_type]))
        df = pd.read_excel(
            cases['CASE_ROOT'].joinpath(
                r'input/parent_fins/' + str(company_type) + ' - financials #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank', 'company_name', 'bvd9', 'bvd_id', 'country_iso', 'NACE_code', 'NACE_desc', 'year_lastav']
                  + ['rnd_y_lastav', 'Emp_number_y_lastav', 'operating_revenue_y_lastav', 'net_sales_y_lastav']
                  + ['rnd_y' + str(YY) for YY in range(10, 20)[::-1]],
            na_values='n.a.',
            dtype={
                **{col: str for col in ['company_name', 'bvd9', 'bvd_id', 'country_iso', 'NACE_code', 'NACE_desc']},
                **{col: float for col in ['rnd_y_lastav', 'operating_revenue_y_lastav', 'net_sales_y_lastav']
                   + ['rnd_y' + str(YY) for YY in range(10, 20)]
                   }
            }
        ).drop(columns=['rank', 'NACE_code', 'NACE_desc', 'year_lastav'])

        # Consolidate subsidiaries financials
        parent_fin = parent_fin.append(df)

    parent_fin = parent_fin.dropna(subset=['rnd_y' + str(YY) for YY in range(10, 20)], how='all')

    for rnd_cols in ['rnd_y' + str(YY) for YY in range(10, 20)]:
        parent_fin[parent_fin[rnd_cols] < 0] = 0

    parent_fin_select = parent_fin[parent_fin['bvd9'].isin(select_comp['bvd9'])]

    report['With financials'] = {'total_bvd9': parent_fin['bvd9'].nunique(),
                                 'total_rnd_y' + str(cases['YEAR_LASTAV'])[-2:]: parent_fin[
                                     'rnd_y' + str(cases['YEAR_LASTAV'])[-2:]].sum(),
                                 'selected_bvd9': parent_fin_select['bvd9'].nunique(),
                                 'selected_rnd_y' + str(cases['YEAR_LASTAV'])[-2:]: parent_fin_select[
                                     'rnd_y' + str(cases['YEAR_LASTAV'])[-2:]].sum()
                                 }

    # Merging subsidiary country_map for allocation to world player categories and countries
    merged = pd.merge(
        parent_fin_select, country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='country_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    # Save it as csv
    merged.to_csv(files['OUTPUT'][company_type]['FIN_EXT']['PARENTS'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return report


def select_subs(cases, files, company_type):
    """
    Consolidate a unique list of subsidiaries
    :param cases:
    :param cases:
    :param files:
    :param files:
    :return: analytical report
    """
    # Initialize DF
    subs = pd.DataFrame()
    report = {}

    print('Read subsidiary input tables')

    # Read ORBIS input list for subsidiaries
    for number in list(range(1, cases['SUBS_ID_FILE_N'][company_type] + 1)):
        print('File #' + str(number) + '/' + str(cases['SUBS_ID_FILE_N'][company_type]))
        df = pd.read_excel(
            cases['CASE_ROOT'].joinpath(
                r'input/sub_ids/' + str(company_type) + ' subsidiaries - identification #' + str(number) + '.xlsx'),
            sheet_name='Results',
            na_values='No data fulfill your filter criteria',
            names=['rank', 'company_name', 'bvd9', 'bvd_id', 'group_subs_Count', 'sub_company_name',
                   'sub_bvd9', 'sub_bvd_id', 'subs_lvl'],
            dtype={
                **{col: str for col in
                   ['rank', 'company_name', 'bvd9', 'bvd_id', 'subsidiary_name', 'sub_bvd9',
                    'sub_bvd_id']},
                'group_subs_Count': pd.Int64Dtype(),
                'subs_lvl': pd.Int8Dtype()}
        ).drop(columns=['rank', 'subs_lvl', 'group_subs_Count'])

        # Consolidate list of subsidiaries
        subs = subs.append(df)

    # Drops not bvd identified subsidiaries and (group,subs) duplicates
    subs = subs.dropna(subset=['bvd9', 'sub_bvd9']).drop_duplicates(['bvd9', 'sub_bvd9'], keep='first')

    report['Claimed by parent companies'] = {'selected_bvd9': subs['bvd9'].nunique(),
                                             'sub_bvd9_in_selected_bvd9': subs['sub_bvd9'].count().sum(),
                                             'unique_sub_bvd9': subs['sub_bvd9'].nunique()
                                             }

    print('Save subsidiaries output file ...')

    # Save it as csv
    subs.to_csv(files['OUTPUT'][company_type]['ID_EXT']['SUBS'],
                index=False,
                columns=['company_name', 'bvd9', 'bvd_id', 'sub_company_name', 'sub_bvd9', 'sub_bvd_id'
                         ],
                float_format='%.10f',
                na_rep='n.a.'
                )

    return report


def load_subs_fin(cases, files, select_subs, country_map, company_type):
    """
    Loads financials for subsidiaries
    :param cases:
    :param cases:
    :param files:
    :param files:
    :param select_subs:
    :param select_subs:
    :param country_map:
    :param country_map:
    :return: Analytical report
    """
    subs_fin = pd.DataFrame()
    report = {}

    print('Read subsidiaries financials input tables')

    # Read ORBIS input list for subsidiaries financials
    for number in list(range(1, cases['SUBS_FIN_FILE_N'][company_type] + 1)):
        print('File #' + str(number) + '/' + str(cases['SUBS_FIN_FILE_N'][company_type]))
        df = pd.read_excel(
            cases['CASE_ROOT'].joinpath(
                r'input/sub_fins/' + str(company_type) + ' subsidiaries - financials #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank', 'sub_company_name', 'sub_bvd9', 'sub_bvd_id', 'country_iso', 'NACE_code', 'NACE_desc',
                   'year_lastavail']
                  + ['operating_revenue_y' + str(YY) for YY in range(10, 20)[::-1]]
                  + ['trade_desc', 'products&services_desc', 'full_overview_desc'],
            na_values='n.a.',
            dtype={
                **{col: str for col in
                   ['sub_company_name', 'sub_bvd9', 'sub_bvd_id', 'country_iso', 'NACE_code', 'NACE_desc',
                    'trade_desc', 'products&services_desc', 'full_overview_desc']},
                **{col: float for col in ['operating_revenue_y' + str(YY) for YY in range(10, 20)[::-1]]}
            }
        ).drop(columns=['rank', 'NACE_code', 'NACE_desc', 'year_lastavail'])

        # Consolidate subsidiaries financials
        subs_fin = subs_fin.append(df)

    subs_fin = subs_fin[subs_fin['sub_bvd9'].isin(select_subs['sub_bvd9'])]

    subs_fin = subs_fin.drop_duplicates('sub_bvd9')

    subs_fin_w_fin = subs_fin.dropna(
        subset=['operating_revenue_y' + str(YY) for YY in range(10, 20)[::-1]],
        how='all')

    report['Returned by ORBIS'] = {'sub_bvd9_in_selected_bvd9': subs_fin['sub_bvd9'].count().sum(),
                                   'unique_sub_bvd9': subs_fin['sub_bvd9'].nunique(),
                                   'unique_has_fin': subs_fin_w_fin['sub_bvd9'].nunique(),
                                   }

    # Merging subsidiary country_map for allocation to world player categories and countries
    merged = pd.merge(
        subs_fin_w_fin, country_map[['country_2DID_iso', 'country_3DID_iso', 'region', 'world_player']],
        left_on='country_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    # Save it as csv
    merged.to_csv(files['OUTPUT'][company_type]['FIN_EXT']['SUBS'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return report


def filter_comps_and_subs(cases, files, select_subs, subs_fin, company_type):
    """
    Add bolean masks for the implementation of different rnd calculation method
    keep_all: Keep all parent companies and all subsidiaries
    keep_comps: Keep all parent companies and exclude subsidiaries that are parent companies from subsidiaries list
    keep_subs: Exclude parent companies that are a subsidiary from companies list and keep all subsidiaries
    :param cases:
    :param cases:
    :param files:
    :param files:
    :param subs_fin:
    :param subs_fin:
    :param select_subs: Consolidated dataframe of subsidiary identification and mapping to companies
    :return: analytical report
    """
    report = {}

    print('Screen companies and subsidiaries lists')

    # Flag parent companies that are a subsidiary of another parent company and vice versa
    select_subs['is_comp_a_sub'] = select_subs['bvd9'].isin(select_subs['sub_bvd9'])
    select_subs['is_sub_a_comp'] = select_subs['sub_bvd9'].isin(select_subs['bvd9'])
    select_subs['has_fin'] = select_subs['sub_bvd9'].isin(subs_fin['sub_bvd9'])

    # Flag subsidiaries that are subsidiaries of multiple parent companies
    select_subs['is_sub_a_duplicate'] = select_subs.duplicated(subset='sub_bvd9', keep=False)

    select_subs['keep_all'] = True
    select_subs['keep_comps'] = select_subs['is_sub_a_comp'] == False
    select_subs['keep_subs'] = select_subs['is_comp_a_sub'] == False

    for method in cases['METHODS']:
        print('Flag strategy: ' + str(method))

        report['From ORBIS with applied method: ' + str(method)] = {
            'selected_bvd9': select_subs['bvd9'][select_subs[method] == True].nunique(),
            'unique_sub_bvd9': select_subs['sub_bvd9'][select_subs[method] == True].nunique(),
            'unique_has_fin': select_subs['sub_bvd9'][
                (select_subs[method] == True) & (select_subs['has_fin'] == True)].nunique()
        }

    print('Save companies and subsidiaries output files with filters ...')

    # Merging subsidiary country_map for allocation to world player categories and countries
    merged = pd.merge(
        select_subs, subs_fin[['sub_bvd9', 'country_2DID_iso', 'country_3DID_iso', 'region', 'world_player']],
        left_on='sub_bvd9', right_on='sub_bvd9',
        how='left',
        suffixes=(False, False)
    ).rename(
        columns={'country_2DID_iso': 'sub_country_2DID_iso',
                 'country_3DID_iso': 'sub_country_3DID_iso',
                 'region': 'sub_region',
                 'world_player': 'sub_world_player'}
    )

    # Save it as csv
    merged.to_csv(files['OUTPUT'][company_type]['METHOD_EXT']['SUBS'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return report


def screen_subs(case, files, keywords, subs_fin, company_type):
    categories = list(keywords.keys())

    report = {}

    for category in categories:

        subs_fin[category] = False

        for keyword in keywords[category]:
            subs_fin[category] |= subs_fin['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  subs_fin['products&services_desc'].str.contains(keyword, case=False, regex=False) | \
                                  subs_fin['full_overview_desc'].str.contains(keyword, case=False, regex=False)

    screen_subs = subs_fin.loc[:, ['sub_company_name', 'sub_bvd9', 'sub_bvd_id'] + categories]

    screen_subs['sub_turnover'] = subs_fin.loc[:, ['operating_revenue_y' + str(YY) for YY in range(10, 20)]].sum(axis=1)

    screen_subs['keyword_mask'] = list(
        map(bool, subs_fin[[cat for cat in categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    screen_subs['sub_turnover_masked'] = screen_subs['sub_turnover'].mask(~screen_subs['keyword_mask'])

    # screen_subs['sub_turnover_y18'] = subs_fin[['operating_revenue_y18']]
    # screen_subs['sub_turnover_y18_masked'] = screen_subs['sub_turnover_y18'].mask(~screen_subs['keyword_mask'])

    report['Returned by ORBIS'] = {
        'unique_is_matching_a_keyword': screen_subs['sub_bvd9'][screen_subs['keyword_mask'] == True].nunique()
    }

    # Save it as csv
    screen_subs.to_csv(files['OUTPUT'][company_type]['SCREEN_EXT']['SUBS'],
                       index=False,
                       columns=['sub_bvd9', 'sub_bvd_id', 'sub_company_name', 'keyword_mask', 'sub_turnover_masked',
                                'sub_turnover', 'keyword_mask'] + [cat for cat in categories],
                       float_format='%.10f',
                       na_rep='n.a.'
                       )

    return report


def compute_sub_exposure(cases, files, select_subs, screen_subs, subs_fin, company_type):
    sub_exposure_conso = pd.DataFrame()
    parent_exposure_conso = pd.DataFrame()
    report_keyword_match = {}
    report_exposure = {'at_subsidiary_level': {}, 'at_parent_level': {}}

    for method in cases['METHODS']:
        print('Compute exposure for strategy: ' + str(method))
        sub_exposure = pd.DataFrame()

        # Merging selected subsidiaries by method with masked turnover and turnover
        sub_exposure = pd.merge(
            select_subs[select_subs[method] == True], screen_subs,
            left_on=['sub_bvd9', 'sub_company_name'], right_on=['sub_bvd9', 'sub_company_name'],
            how='left'
        )

        # Calculating group exposure
        parent_exposure = sub_exposure[
            ['bvd9', 'sub_turnover_masked', 'sub_turnover']
        ].groupby(['bvd9']).sum().rename(
            columns={'sub_turnover': 'total_sub_turnover_in_parent',
                     'sub_turnover_masked': 'total_sub_turnover_masked_in_parent'}
        )

        parent_exposure['parent_exposure'] = parent_exposure['total_sub_turnover_masked_in_parent'] / \
                                             parent_exposure['total_sub_turnover_in_parent']

        parent_exposure['method'] = str(method)

        parent_exposure.reset_index(inplace=True)

        parent_exposure_conso = parent_exposure_conso.append(parent_exposure)

        # Calculating subsidiary level exposure
        sub_exposure = pd.merge(
            sub_exposure, parent_exposure[
                ['bvd9', 'total_sub_turnover_masked_in_parent', 'total_sub_turnover_in_parent',
                 'parent_exposure']],
            left_on='bvd9', right_on='bvd9',
            how='left'
        )

        sub_exposure['sub_exposure'] = sub_exposure['sub_turnover_masked'] / sub_exposure[
            'total_sub_turnover_in_parent']

        sub_exposure['method'] = str(method)

        sub_exposure.dropna(subset=['parent_exposure', 'sub_turnover'], inplace=True)

        report_keyword_match['From ORBIS with applied method: ' + str(method)] = {
            'sub_bvd9_in_selected_bvd9': select_subs['sub_bvd9'][select_subs[method] == True].count().sum(),
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

    # Save output tables
    parent_exposure_conso.to_csv(files['OUTPUT'][company_type]['EXPO_EXT']['PARENTS'],
                                 index=False,
                                 float_format='%.10f',
                                 na_rep='n.a.',
                                 columns=['bvd9', 'total_sub_turnover_masked_in_parent',
                                          'total_sub_turnover_in_parent', 'parent_exposure', 'method'
                                          ]
                                 )

    sub_exposure_conso.to_csv(files['OUTPUT'][company_type]['EXPO_EXT']['SUBS'],
                              index=False,
                              float_format='%.10f',
                              na_rep='n.a.',
                              columns=['bvd9',
                                       'company_name',
                                       'total_sub_turnover_masked_in_parent',
                                       'total_sub_turnover_in_parent',
                                       'parent_exposure',
                                       'sub_bvd9',
                                       'sub_company_name',
                                       'sub_country_2DID_iso',
                                       'sub_country_3DID_iso',
                                       'sub_world_player',
                                       'sub_turnover',
                                       'sub_turnover_masked',
                                       'sub_exposure',
                                       'method'
                                       ]
                              )

    return report_keyword_match, report_exposure


def compute_parent_rnd(cases, files, parent_exposure, parent_fin, company_type):
    parent_rnd_conso = pd.DataFrame()

    report_parent_rnd = {}

    parent_rnd = pd.merge(parent_exposure, parent_fin,
                          left_on='bvd9', right_on='bvd9',
                          how='left'
                          )

    for method in cases['METHODS']:
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating group level rnd
        parent_rnd_method = parent_rnd_method.melt(
            id_vars=['bvd9', 'total_sub_turnover_masked_in_parent', 'total_sub_turnover_in_parent',
                     'parent_exposure', 'company_name', 'country_2DID_iso', 'country_3DID_iso',
                     'world_player'],
            value_vars=['rnd_y' + str(YY) for YY in range(10, 20)[::-1]],
            var_name='rnd_label', value_name='parent_rnd')

        parent_rnd_method['year'] = [int('20' + s[-2:]) for s in parent_rnd_method['rnd_label']]

        parent_rnd_method['parent_rnd_final'] = parent_rnd_method['parent_rnd'] * parent_rnd_method[
            'parent_exposure']

        parent_rnd_method['method'] = str(method)

        # parent_rnd_method.dropna(subset=['parent_exposure', 'parent_rnd', 'parent_rnd_final'], how='any',
        #                             inplace=True)

        parent_rnd_conso = parent_rnd_conso.append(parent_rnd_method)

        report_parent_rnd.update(
            pd.DataFrame.to_dict(
                parent_rnd_method[['year', 'parent_rnd_final']].groupby(
                    ['year']).sum().rename(columns={'parent_rnd_final': 'with_method: ' + str(method)})
            )
        )

    # parent_rnd_conso.dropna(subset=['parent_rnd_final'], inplace=True)

    parent_rnd_conso.to_csv(files['OUTPUT'][company_type]['RND_EXT']['PARENTS'],
                            index=False,
                            columns=['bvd9', 'company_name', 'country_2DID_iso', 'country_3DID_iso',
                                     'world_player', 'total_sub_turnover_masked_in_parent',
                                     'total_sub_turnover_in_parent', 'parent_exposure', 'year',
                                     'parent_rnd',
                                     'parent_rnd_final', 'method'
                                     ],
                            float_format='%.10f',
                            na_rep='n.a.'
                            )

    return report_parent_rnd


def compute_sub_rnd(cases, files, sub_exposure, parent_rnd, company_type):
    sub_rnd_conso = pd.DataFrame()

    report_sub_rnd = {}

    for method in cases['METHODS']:
        sub_rnd = pd.DataFrame()

        sub_exposure_method = sub_exposure[sub_exposure['method'] == method]
        parent_rnd_method = parent_rnd[parent_rnd['method'] == method]

        # Calculating subsidiary level rnd
        sub_rnd = pd.merge(
            sub_exposure_method, parent_rnd_method[['bvd9', 'parent_rnd', 'year', 'parent_rnd_final']],
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

        sub_rnd['sub_rnd_final'] = sub_rnd['parent_rnd_final'] * sub_rnd['sub_exposure'] / sub_rnd[
            'parent_exposure_from_sub']

        sub_rnd['method'] = str(method)

        sub_rnd_conso = sub_rnd_conso.append(sub_rnd)

        report_sub_rnd.update(
            pd.DataFrame.to_dict(
                sub_rnd[['year', 'sub_rnd_final']].groupby(['year']).sum().rename(
                    columns={'sub_rnd_final': 'with_method: ' + str(method)})
            )
        )

    # sub_rnd_conso.dropna(subset=['sub_rnd_final'], inplace=True)

    # Save output tables
    sub_rnd_conso.to_csv(files['OUTPUT'][company_type]['RND_EXT']['SUBS'],
                         index=False,
                         columns=['bvd9', 'company_name', 'year', 'sub_bvd9', 'sub_company_name',
                                  'sub_country_2DID_iso', 'sub_country_3DID_iso',
                                  'sub_world_player', 'sub_turnover', 'sub_turnover_masked', 'sub_exposure',
                                  'sub_rnd_final', 'method'
                                  ],
                         float_format='%.10f',
                         na_rep='n.a.'
                         )

    return report_sub_rnd
