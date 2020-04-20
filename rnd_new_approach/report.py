from tabulate import tabulate
import pandas as pd
import numpy as np
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
        selected_sub_ids,
        country_map
):
    print('... merge with country data')

    # Get country info for subs
    sub_ids_w_country = pd.merge(
        selected_sub_ids,
        country_map[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
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
        rnd_cluster_cats,
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
        cases,
        rnd_cluster_cats,
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

    # Group at parent level with bvd9
    sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type', 'is_listed_company',
                            'cluster', 'method']

    # sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type', 'is_listed_company',
    #                         'method']

    # Group without bvd9 for soeur_rnd benchmark
    sub_rnd_grouped = sub_rnd_melted.groupby(sub_rnd_grouped_cols).sum()

    sub_rnd_grouped.reset_index(inplace=True)

    sub_rnd_grouped['approach'] = 'NewApp_rnd_2020_GLOBAL_20200415'

    sub_rnd_grouped['technology'] = sub_rnd_grouped['priority'] = sub_rnd_grouped['action'] = '#N/A'

    return sub_rnd_grouped


def merge_n_group_sub_rnd(
        cases,
        rnd_cluster_cats,
        sub_rnd,
        parent_ids,
        parent_guo_ids,
        selected_sub_ids,
        country_map,
        selected_sub_fins
):

    sub_rnd_merged_w_parents = merge_sub_rnd_w_parents(
        sub_rnd,
        parent_ids,
        parent_guo_ids
    )

    sub_rnd_merged_w_countries = merge_sub_rnd_w_countries(
        sub_rnd_merged_w_parents,
        selected_sub_ids,
        country_map
    )

    sub_rnd_merged_w_clusters = merge_sub_rnd_w_clusters(
        rnd_cluster_cats,
        sub_rnd_merged_w_countries,
        selected_sub_fins
    )

    sub_rnd_grouped = melt_n_group_sub_rnd(
        cases,
        rnd_cluster_cats,
        sub_rnd_merged_w_clusters
    )

    sub_rnd_grouped.mask(sub_rnd_grouped['is_listed_company'] == True, 'listed')
    sub_rnd_grouped.mask(sub_rnd_grouped['is_listed_company'] == False, 'unlisted guo50')

    sub_rnd_grouped.rename(columns={'is_listed_company': 'type'}, inplace=True)

    return sub_rnd_grouped


def load_n_group_soeur_rnd(
        cases,
        files
):
    print('... load benchmark table')

    soeur_rnd_grouped = pd.read_csv(
        'https://raw.githubusercontent.com/pysleto/mapping-tables/master/SOEUR_rnd_2019b_20200309.csv',
        error_bad_lines=False)

    print('... and group')

    soeur_rnd_grouped = soeur_rnd_grouped.groupby(['year', 'sub_country_3DID_iso', 'sub_world_player']).sum()

    soeur_rnd_grouped.reset_index(inplace=True)

    soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309'

    soeur_rnd_grouped['method'] = '#N/A'

    soeur_rnd_grouped['type'] = soeur_rnd_grouped['cluster'] = soeur_rnd_grouped['guo_type'] = '#N/A'

    return soeur_rnd_grouped


def load_n_group_MNC_rnd(
        cases,
        files
):
    print('... load benchmark table')

    mnc_rnd_grouped = pd.read_csv(
        'https://raw.githubusercontent.com/pysleto/mapping-tables/master/NewApp_MNC_rnd_2019_20190731.csv',
        error_bad_lines=False)

    mnc_rnd_grouped['approach'] = 'NewApp_MNC_rnd_2019_20190731'

    mnc_rnd_grouped['method'] = '#N/A'

    mnc_rnd_grouped['type'] = mnc_rnd_grouped['cluster'] = mnc_rnd_grouped['guo_type'] = '#N/A'

    return mnc_rnd_grouped