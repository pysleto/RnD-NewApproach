# Import libraries
from pathlib import Path

import pandas as pd
import json

from data_input import file_loader as load

from config import registry as reg

rnd_cluster_cats = [cat for cat in reg.categories if cat not in ['generation', 'rnd']]

# Import mapping tables
ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))

# TODO: Check that the soeur to orbis parent relation is a 1-to-1
# TODO: clean and extensive icb table


def update_n_format_soeur_rnd(soeur_version):
    """
    Load output form soeur_rnd approach
    """
    print('Read soeur_rnd from xls tables ...')

    soeur_rnd = load.soeur_rnd_from_xls(reg.project_path.joinpath(r'data_input', 'SOEUR_RnD', soeur_version + '.xlsx'))

    soeur_rnd = soeur_rnd[(soeur_rnd['action'] != 'z_Others') &
                          (soeur_rnd['technology'] != 'z_Others') &
                          (soeur_rnd['priority'] != 'z_Others')]

    print('Merge with country_map ...')

    # Update group level country data
    rnd_merge = pd.merge(
        soeur_rnd,
        ref_country[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='group_country_2DID_soeur', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    ).drop(columns='country_2DID_soeur')

    rnd_merge.rename(columns={'country_2DID_iso': 'group_country_2DID_iso',
                              'country_3DID_iso': 'group_country_3DID_iso', 'world_player': 'group_world_player'},
                     inplace=True
    )

    # Update sub level country data
    rnd_merge = pd.merge(
        rnd_merge,
        ref_country[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_soeur', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    ).drop(columns='country_2DID_soeur')

    rnd_merge.rename(columns={'country_2DID_iso': 'sub_country_2DID_iso', 'country_3DID_iso': 'sub_country_3DID_iso',
                              'world_player': 'sub_world_player'}, inplace=True
    )

    rnd_merge = rnd_merge[[
        'year',
        'soeur_group_id', 'soeur_group_name',
        'group_country_2DID_soeur', 'group_country_2DID_iso', 'group_world_player',
        'sector_UC', 'group_UC', 'rnd_group_UC',
        'soeur_sub_id', 'soeur_sub_name',
        'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3',
        'sub_country_2DID_soeur', 'sub_country_2DID_iso', 'sub_world_player',
        'technology', 'action', 'priority', 'rnd_clean'
    ]]

    rnd_merge.to_csv(reg.project_path.joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '.csv'),
                     index=False, na_rep='#N/A', encoding='UTF-8'
                     )


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
        ref_country[['country_2DID_iso', 'country_3DID_iso', 'world_player']],
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
                 'sub_conso', 'parent_conso', 'is_in_top', 'is_sub_top', 'is_parent_top', 'method'],
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
    sub_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'guo_type',
                            'sub_conso', 'parent_conso', 'is_in_top', 'is_sub_top', 'is_parent_top',
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