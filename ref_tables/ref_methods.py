# Import libraries
from pathlib import Path

import pandas as pd

import init_config as cfg

from data_input import file_loader as load

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

# Load config files
reg = cfg.load_my_registry()

# TODO: Check that the soeur to orbis parent relation is a 1-to-1
# TODO: clean and extensive icb table


def update_n_format_soeur_rnd(soeur_version):
    """
    Load output form soeur_rnd approach
    """
    print('Read soeur_rnd from xls tables ...')

    soeur_rnd = load.soeur_rnd_from_xls(reg['project_root'].joinpath(r'data_input', 'SOEUR_RnD', soeur_version + '.xlsx'))

    soeur_rnd = soeur_rnd[(soeur_rnd['action'] != 'z_Others') &
                          (soeur_rnd['technology'] != 'z_Others') &
                          (soeur_rnd['priority'] != 'z_Others')]

    print('Merge with country_map ...')

    country_ref = pd.read_csv(reg['country'], error_bad_lines=False, encoding='UTF-8')

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

    rnd_merge.to_csv(reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '.csv'),
                     index=False, na_rep='#N/A', encoding='UTF-8'
                     )
