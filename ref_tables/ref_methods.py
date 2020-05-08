# Import libraries
from pathlib import Path

import pandas as pd

from data_input import file_loader as load


# TODO: clean and extensive icb table

def update_n_format_soeur_rnd(file, reg):
    """
    Load output form soeur_rnd approach
    """
    print('Read soeur_rnd from xls tables ...')

    soeur_rnd = load.soeur_rnd_from_xls(file)

    soeur_rnd = soeur_rnd[(soeur_rnd['action'] != 'z_Others') &
                          (soeur_rnd['technology'] != 'z_Others') &
                          (soeur_rnd['priority'] != 'z_Others')]

    print('Merge with country_map ...')

    country_ref = pd.read_csv(reg['ref_tables']['country'], error_bad_lines=False, encoding='UTF-8')

    # Update group level country data
    rnd_merge = pd.merge(
        soeur_rnd,
        country_ref[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
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
        country_ref[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_soeur', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    ).drop(columns='country_2DID_soeur')

    rnd_merge.rename(columns={'country_2DID_iso': 'sub_country_2DID_iso', 'country_3DID_iso': 'sub_country_3DID_iso',
                              'world_player': 'sub_world_player'}, inplace=True
    )

    return rnd_merge


def merge_w_orbis_parent_ids(soeur_rnd, cases, reg):

    print('Merge with company_table ...')

    company_ref = pd.read_csv(reg['ref_tables']['company'], error_bad_lines=False, encoding='UTF-8')

    company_ref.dropna(subset=['orbis_parent_bvd_name'], inplace=True)

    soeur_rnd = pd.merge(
        soeur_rnd,
        company_ref[['soeur_parent_name', 'orbis_parent_bvd_name', 'orbis_parent_bvd9']],
        left_on='soeur_group_name', right_on='soeur_parent_name',
        how='left',
        suffixes=(False, False)
    ).drop(columns='soeur_parent_name')

    print('Merge with parent_ids ...')

    parent_ids_ref = pd.read_csv(cases['CASE_ROOT'].jointpath('1 - identification - parents.csv'),
                                 error_bad_lines=False,
                                 encoding='UTF-8')

    soeur_rnd = pd.merge(
        soeur_rnd,
        parent_ids_ref[['bvd9', 'country_2DID_iso', 'world_player']],
        left_on='orbis_parent_bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    ).drop(columns='bvd9')

    soeur_rnd.rename(columns={'country_2DID_iso': 'orbis_parent_country_2DID_iso',
                              'world_player': 'orbis_parent_world_player'}, inplace=True
                     )

    return soeur_rnd

