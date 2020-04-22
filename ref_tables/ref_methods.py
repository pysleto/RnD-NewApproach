# Import libraries
from pathlib import Path

import pandas as pd

from data_input import file_loader as load


# TODO: clean and extensive icb table

def update_n_format_soeur_rnd(file):
    """
    Load output form soeur_rnd approach
    """
    print('Read soeur_rnd from xls tables ...')

    soeur_rnd = load.soeur_rnd_from_xls(file)

    soeur_rnd = soeur_rnd[(soeur_rnd['action'] != 'z_Others') &
                          (soeur_rnd['technology'] != 'z_Others') &
                          (soeur_rnd['priority'] != 'z_Others')]

    print('Flag parents embedded in MNC ...')

    # TODO: Upload VCS reference table
    # Flag parents embedded in MNC
    mnc_ids = pd.read_csv(
        Path('C:/Users/Simon/PycharmProjects/rnd-private/ref_tables/mnc_tracking_jrc004_to_newapp_20200420.csv'),
        na_values='#N/A',
        dtype=str
    )

    soeur_rnd['is_embedded_in_MNC'] = soeur_rnd.soeur_group_name.isin(mnc_ids.soeur_group_name)

    # print(soeur_rnd[soeur_rnd['is_embedded_in_MNC'] == True].head())

    print('Merge with country_map ...')

    country_table = pd.read_csv('https://raw.githubusercontent.com/pysleto/mapping-tables/master/country_table.csv',
                                error_bad_lines=False)

    # Update group level country data
    rnd_merge = pd.merge(
        soeur_rnd,
        country_table[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
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
        country_table[['country_2DID_soeur', 'country_2DID_iso', 'country_3DID_iso', 'world_player']],
        left_on='sub_country_2DID_soeur', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    ).drop(columns='country_2DID_soeur')

    rnd_merge.rename(columns={'country_2DID_iso': 'sub_country_2DID_iso', 'country_3DID_iso': 'sub_country_3DID_iso',
                              'world_player': 'sub_world_player'}, inplace=True
    )

    return rnd_merge