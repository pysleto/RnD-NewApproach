import pandas as pd
from pathlib import Path


def mnc_soeur_rnd(root, file_name):

    print('Read SOEUR MNC RnD Exposure excel table')

    file_path = root.joinpath(file_name)

    mnc_table_cols = ['soeur_group_id',
                      'soeur_group_name',
                      'soeur_group_country_2DID',
                      'soeur_group_world_player',
                      'icb_3_name',
                      'year',
                      'soeur_group_rnd',
                      'soeur_group_rnd_from_group_uc',
                      'soeur_group_rnd_from_sector_uc',
                      'soeur_group_norm',
                      'soeur_group_exposure_from_group_uc',
                      'soeur_group_exposure_from_sector_uc']

    # step if countries are in country-table

    mnc_table = pd.read_excel(
        file_path,
        sheet_name='MNCs_R&D_Exposure_v2',
        names=mnc_table_cols,
        na_values='#N/A',
        dtype={
            **{col: str for col in mnc_table_cols[:5]},
            **{col: float for col in mnc_table_cols[6:]}
        }
    )

    # mnc_table.set_index(keys='soeur_group_name', inplace=True)

    return mnc_table[['soeur_group_id',
                      'soeur_group_name',
                      'soeur_group_country_2DID',
                      'icb_3_name',
                      'year',
                      'soeur_group_rnd',
                      'soeur_group_rnd_from_group_uc',
                      'soeur_group_rnd_from_sector_uc',
                      'soeur_group_exposure_from_group_uc',
                      'soeur_group_exposure_from_sector_uc']]


def mnc_newapp_rnd(root, file_name, method):

    print('Read NewApp parent RnD csv table')

    file_path = root.joinpath(file_name)

    mnc_table_cols = [
        'bvd9', 'year', 'orbis_parent_oprev', 'orbis_parent_rnd', 'newapp_parent_exposure', 'newapp_parent_rnd_clean',
        'method'
    ]

    mnc_table = pd.read_csv(
        file_path,
        names=mnc_table_cols,
        header=0,
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    mnc_table = mnc_table[mnc_table['method'] == method]

    return mnc_table[mnc_table_cols[:-1]]