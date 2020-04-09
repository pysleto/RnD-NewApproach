from tabulate import tabulate
import pandas as pd
import numpy as np
import input
import json


def convert(o):
    if isinstance(o, np.int32): return int(o)


def update(report, cases):
    """
    Update a json file with reporting outputs and pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    print('Update report.json file ...')

    with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'w') as file:
        json.dump(report, file, indent=4, default=convert)


def pprint(report, cases):
    """
    Pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    with open(cases['CASE_ROOT'].joinpath(r'report.txt'), 'w') as file:
        file.write('INITIALISE\n\n')

        json.dump(report['initialisation'], file, indent=4, default=convert)

        file.write('\n\n')

        file.write('NB: RnD in EUR million\n\n')

        for company_type in cases['COMPANY_TYPES']:
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


def load_soeur_rnd(cases, files, country_map):
    """
    Load output form soeur_rnd approach
    """

    soeur_rnd = pd.DataFrame()

    report = {}

    print('Read soeur_rnd from source tables')

    soeur_rnd = input.import_soeur_rnd_from_xls(files['SOEUR_RND']['SOURCE'])

    soeur_rnd = soeur_rnd[(soeur_rnd['action'] != 'z_Others') &
                          (soeur_rnd['technology'] != 'z_Others') &
                          (soeur_rnd['priority'] != 'z_Others')]

    # Define column ids

    print('Merge with country_map ...')

    # Merge with country_map
    rnd_merge = pd.merge(
        soeur_rnd,
        country_map[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='country_2DID_soeur', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    ).drop(columns='country_2DID_soeur')

    rnd_merge.rename(columns={'country_2DID_iso': 'sub_country_2DID_iso', 'country_3DID_iso': 'sub_country_3DID_iso',
                              'world_player': 'sub_world_player'}, inplace=True
    )

    soeur_rnd_cols = ['jrc_id', 'sub_company_name', 'sub_country_2DID_iso', 'sub_country_3DID_iso', 'sub_world_player',
                      'technology', 'action', 'priority', 'year', 'sub_rnd_clean']

    print('Save soeur_rnd output files ...')

    # Save output table of selected parent companies
    rnd_merge.to_csv(files['SOEUR_RND']['ROOT'].joinpath(files['SOEUR_RND']['VERSION'] + '- full.csv'),
                     columns=soeur_rnd_cols,
                     float_format='%.10f',
                     index=False,
                     na_rep='n.a.'
                     )

    return rnd_merge


def group_sub_rnd_by_approach(
        cases,
        files,
        keywords,
        soeur_rnd_grouped,
        sub_rnd,
        parent_ids,
        parent_guo_ids,
        selected_sub_ids,
        selected_sub_fins,
        country_map):
    """
    :param cases:
    :param files:
    :return:
    """

    # Initialisation
    rnd_conso = pd.DataFrame()

    sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type', 'is_listed_company',
                            'cluster', 'method']

    soeur_rnd_grouped_cols = list(soeur_rnd_grouped.columns)

    print('Consolidate rnd by approach')

    # Group soeur_rnd
    print('... soeur_rnd')

    soeur_rnd_grouped['approach'] = files['SOEUR_RND']['VERSION']

    soeur_rnd_grouped['method'] = 'keep_all'

    soeur_rnd_grouped['is_listed_company'] = soeur_rnd_grouped['cluster'] = soeur_rnd_grouped['guo_type'] = 'n.a.'

    rnd_conso = rnd_conso.append(soeur_rnd_grouped)

    # Group sub_rnd
    print('... new approach')

    # Get company type info for subs
    sub_rnd_merged = pd.merge(
        sub_rnd,
        parent_ids[['bvd9', 'is_listed_company']],
        left_on='bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    ).drop(columns=['parent_rnd_clean', 'parent_exposure_from_sub', 'sub_exposure'])

    # Get ultimate owner company type info for subs
    sub_rnd_merged = pd.merge(
        sub_rnd_merged,
        parent_guo_ids[['guo_bvd9', 'guo_type']],
        left_on='bvd9', right_on='guo_bvd9',
        how='left',
        suffixes=(False, False)
    ).drop(columns=['guo_bvd9'])

    # Get country info for subs
    sub_rnd_merged = pd.merge(
        sub_rnd_merged,
        selected_sub_ids[['sub_bvd9', 'sub_country_2DID_iso']],
        left_on='sub_bvd9', right_on='sub_bvd9',
        how='left',
        suffixes=(False, False)
    )

    sub_rnd_merged = pd.merge(
        sub_rnd_merged,
        country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    ).drop(columns=['country_2DID_iso', 'sub_country_2DID_iso'])

    sub_rnd_merged.rename(
        columns={'country_3DID_iso': 'sub_country_3DID_iso',
                 'world_player': 'sub_world_player'}, inplace=True
        )

    # Get keyword info for subs
    categories = list(keywords.keys())

    sub_rnd_merged = pd.merge(
        sub_rnd_merged,
        selected_sub_fins[['sub_bvd9', 'keyword_mask'] + [cat for cat in categories]],
        left_on='sub_bvd9', right_on='sub_bvd9',
        how='left',
        suffixes=(False, False)
    )

    # Compute a keyword based share for each cluster and apply to subs_rnd
    sub_rnd_merged['keyword_mask'] = sub_rnd_merged[[cat for cat in categories]].sum(axis=1)

    for category in categories:
        sub_rnd_merged[category] = sub_rnd_merged['sub_rnd_clean'] * sub_rnd_merged[category] / sub_rnd_merged[
            'keyword_mask']

    sub_rnd_merged.drop(columns=['keyword_mask', 'sub_rnd_clean'], inplace=True)
    
    sub_rnd_melted = sub_rnd_merged.melt(
        id_vars=['bvd9', 'sub_bvd9', 'year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type',
                 'is_listed_company', 'method'],
        value_vars=[cat for cat in categories],
        var_name='cluster', value_name='sub_rnd_clean')


    # Group with bvd9
    sub_rnd_grouped_w_bvd9 = sub_rnd_melted.groupby(['bvd9'] + sub_rnd_grouped_cols).sum()

    sub_rnd_grouped_w_bvd9.rename(
        columns={'sub_rnd_clean': 'parent_rnd_clean'}, inplace=True
        )

    sub_rnd_grouped_w_bvd9.reset_index(inplace=True)

    # Group without bvd9 for soeur_rnd benchmark
    sub_rnd_grouped = sub_rnd_melted.groupby(sub_rnd_grouped_cols).sum()

    sub_rnd_grouped.reset_index(inplace=True)

    sub_rnd_grouped['approach'] = 'new_approach'

    sub_rnd_grouped['technology'] = sub_rnd_grouped['priority'] = sub_rnd_grouped['action'] = 'n.a.'

    rnd_conso = rnd_conso.append(sub_rnd_grouped)

    rnd_conso.loc[rnd_conso['is_listed_company'] == True, 'type'] = 'listed'
    rnd_conso.loc[rnd_conso['is_listed_company'] == False, 'type'] = 'unlisted'

    # Save output tables
    rnd_conso.to_csv(files['FINAL']['BY_APPROACH'],
                     columns=['approach', 'method', 'year', 'sub_rnd_clean', 'guo_type', 'type', 'sub_world_player',
                              'sub_country_3DID_iso', 'cluster', 'technology', 'priority', 'action'],
                     float_format='%.10f',
                     index=False,
                     na_rep='n.a.'
                     )

    return sub_rnd_grouped_w_bvd9[['bvd9', 'year', 'is_listed_company', 'cluster', 'method', 'parent_rnd_clean']]

